"""
run_all.py

crawler_commands.txt 의 각 명령을 전역 인덱스 기반으로 실행.
키워드 → region 코드 매핑 후 Firestore에 함께 저장.

동작:
  1. Firestore 트랜잭션으로 다음 인덱스를 원자적으로 클레임 (3-job 안전)
  2. 키워드가 이미 done 이면 skip (resume)
  3. naver_map_crawler.crawl() 직접 호출
  4. region_mapper 로 지역 코드 매핑
  5. 성공 → Firestore places + (옵션) xlsx 백업
  6. 실패(429 등) → 지수 백오프 재시도
  7. MAX_RUNTIME_SEC 초과 또는 명령 소진 시 안전 종료

환경변수:
  COMMANDS_FILE       기본 crawler_commands.txt
  SLEEP_SEC           키워드 간 sleep (기본 90)
  MAX_RETRIES         재시도 횟수 (기본 3)
  RETRY_BASE_SLEEP    재시도 베이스 sleep (기본 180, 지수 백오프)
  MAX_RUNTIME_SEC     최대 실행 시간 (기본 20400 = 5h 40m)
  RESET_PROGRESS      "1" 이면 시작 시 인덱스 초기화
  SAVE_XLSX           "1" 이면 xlsx 백업 저장 (기본 0)
"""
from __future__ import annotations

import os
import sys
import time
from datetime import datetime

import pandas as pd

import naver_map_crawler as nmc
from naver_map_crawler import crawl, EXCEL_COLUMNS

try:
    from command_parser import parse_command
except ModuleNotFoundError:
    import re as _re
    _CMD_RE = _re.compile(r'"\s*([^"]+?)\s*"\s*(?:--max\s+(\d+))?')
    def parse_command(cmd: str) -> tuple[str, int]:
        m = _CMD_RE.search(cmd)
        if not m:
            raise ValueError(f"명령 파싱 실패: {cmd}")
        return m.group(1).strip(), int(m.group(2)) if m.group(2) else 50
from region_mapper import map_keyword_to_region
from firestore_store import (
    init_firebase,
    claim_next_index,
    is_keyword_done,
    upload_rows_to_firestore,
    mark_job_done,
    mark_job_failed,
    get_progress,
    reset_progress,
)


COMMANDS_FILE = os.environ.get("COMMANDS_FILE", "crawler_commands.txt")
SLEEP_SEC = int(os.environ.get("SLEEP_SEC", "90"))
MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "3"))
RETRY_BASE_SLEEP = int(os.environ.get("RETRY_BASE_SLEEP", "180"))
MAX_RUNTIME_SEC = int(os.environ.get("MAX_RUNTIME_SEC", "20400"))
SAVE_XLSX = os.environ.get("SAVE_XLSX", "0") == "1"
OUTPUT_DIR = "output"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def log(msg: str) -> None:
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


def dump_naver_response(keyword: str) -> None:
    """0건 실패 시 네이버 실제 응답 확인 — 차단 여부 판단용."""
    import requests as _req
    from urllib.parse import quote
    url = f"https://pcmap.place.naver.com/place/list?query={quote(keyword)}&page=1"
    try:
        resp = _req.get(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
            "Referer": "https://map.naver.com/",
            "Accept-Language": "ko-KR,ko;q=0.9",
        }, timeout=15)
        body = resp.text
        log(f"  🔍 [응답 진단] HTTP {resp.status_code} / {len(body)}bytes")
        # 차단/정상 신호 탐지
        checks = [
            ("APOLLO_STATE",    "✅ 정상 (Apollo state 있음)"),
            ("__NEXT_DATA__",   "✅ 정상 (Next.js data 있음)"),
            ("비정상적인 접근",  "🚫 비정상 접근 차단"),
            ("일시적으로 차단",  "🚫 일시 차단"),
            ("잠시 후 다시",    "🚫 일시 차단"),
            ("captcha",        "🚫 캡차 감지"),
        ]
        found = [label for signal, label in checks if signal.lower() in body.lower()]
        if found:
            log(f"  🔍 [응답 신호] {' | '.join(found)}")
        else:
            log(f"  🔍 [응답 신호] 판단 불가 — 앞 300자: {body[:300]}")
    except Exception as e:
        log(f"  🔍 [응답 진단 실패] {e}")


# 연속 0건 실패 카운터 (모듈 레벨 — 같은 프로세스 내 공유)
_consecutive_empty = 0
BLOCK_THRESHOLD = 3      # 이 횟수 연속 0건이면 IP 차단으로 판단
BLOCK_SLEEP = 1800       # 차단 판단 시 대기 시간 (30분)


def run_one_keyword(keyword: str, max_items: int) -> tuple[bool, list, str]:
    """단일 키워드 크롤링 + 재시도.

    Returns: (success, rows, error_msg)
    """
    global _consecutive_empty

    # 연속 0건이 BLOCK_THRESHOLD 이상이면 IP 차단으로 판단 → 장시간 대기
    if _consecutive_empty >= BLOCK_THRESHOLD:
        log(f"  🚫 연속 {_consecutive_empty}회 0건 — IP 차단 의심. {BLOCK_SLEEP}초({BLOCK_SLEEP//60}분) 대기...")
        time.sleep(BLOCK_SLEEP)
        _consecutive_empty = 0  # 대기 후 리셋해서 다시 시도

    last_err = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            rows = crawl(keyword, max_items, fetch_detail=True, debug_dump=False)
            if not rows:
                last_err = "수집 결과 0건"
                # 첫 실패 시에만 응답 진단 (매번 하면 부하)
                if attempt == 1:
                    dump_naver_response(keyword)
            else:
                _consecutive_empty = 0  # 성공 시 카운터 리셋
                return True, rows, ""
        except Exception as e:
            err_str = str(e)
            if "429" in err_str:
                last_err = "429 Too Many Requests"
            else:
                last_err = f"{type(e).__name__}: {err_str}"
            _consecutive_empty = 0  # 429는 명시적 에러라 차단 카운터와 별개

        if attempt >= MAX_RETRIES:
            break

        if "429" in last_err:
            backoff = RETRY_BASE_SLEEP * (2 ** (attempt - 1))  # 180 → 360 → 720
        elif "0건" in last_err:
            # 0건은 짧게 1번만 재시도 — 차단 상태면 기다려도 의미 없음
            backoff = 60
        else:
            backoff = 60 * attempt

        log(f"  ⚠  실패(시도 {attempt}/{MAX_RETRIES}): {last_err[:200]}")
        log(f"  ⏳ {backoff}초 대기 후 재시도...")
        time.sleep(backoff)

    # 0건 실패 카운터 증가
    if "0건" in last_err:
        _consecutive_empty += 1
        log(f"  📊 연속 0건 횟수: {_consecutive_empty}/{BLOCK_THRESHOLD}")

    return False, [], last_err


def save_excel_with_region(
    rows: list,
    path: str,
    region_code: str | None,
    region_name: str | None,
    sub_region_code: str | None,
    sub_region_name: str | None,
) -> None:
    """크롤러의 save_excel 과 동일하되, region 컬럼 4개 추가."""
    extra_cols = ["지역코드", "지역명", "시군구코드", "시군구명"]
    columns = EXCEL_COLUMNS + extra_cols

    records = []
    for r in rows:
        d = r.to_dict() if hasattr(r, "to_dict") else r
        d["지역코드"] = region_code or ""
        d["지역명"] = region_name or ""
        d["시군구코드"] = sub_region_code or ""
        d["시군구명"] = sub_region_name or ""
        records.append(d)

    df = pd.DataFrame(records, columns=columns)
    for col in ("방문자 리뷰수", "방문자 평점", "블로그 리뷰수", "사진리뷰수", "위도", "경도"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
        ws = writer.sheets["Sheet1"]
        for i, col in enumerate(df.columns, start=1):
            ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = max(
                12, min(40, len(col) + 4)
            )


def main() -> int:
    init_firebase()

    if os.environ.get("RESET_PROGRESS") == "1":
        log("⚠  RESET_PROGRESS=1 — 진행 인덱스 초기화")
        reset_progress()

    with open(COMMANDS_FILE, "r", encoding="utf-8") as f:
        commands = [
            line.strip() for line in f
            if line.strip() and not line.startswith("#")
        ]
    total = len(commands)

    progress = get_progress()
    log(f"전체 {total}개. last_claimed_index={progress.get('last_claimed_index', -1)}")

    started = time.time()
    processed = succeeded = failed = skipped = 0

    while True:
        elapsed = time.time() - started
        if elapsed > MAX_RUNTIME_SEC:
            log(f"⏱  MAX_RUNTIME_SEC({MAX_RUNTIME_SEC}s) 초과 — 종료")
            break

        idx = claim_next_index(total)
        if idx is None:
            log("✅ 모든 명령 처리 완료")
            break

        cmd = commands[idx]
        try:
            keyword, max_items = parse_command(cmd)
        except ValueError as e:
            log(f"[idx={idx}] ❌ 명령 파싱 실패: {e}")
            failed += 1
            continue

        processed += 1

        # resume: 이미 done 이면 skip
        if is_keyword_done(keyword):
            log(f"[idx={idx}] ⏭  이미 완료: {keyword}")
            skipped += 1
            continue

        # region 매핑
        region_code, region_name, sub_region_code, sub_region_name = map_keyword_to_region(keyword)
        if not region_code:
            log(f"[idx={idx}] ⚠  region 매칭 실패: {keyword!r} (그래도 진행)")
        elif not sub_region_code:
            log(f"[idx={idx}] ⚠  sub_region 매칭 실패: {keyword!r} → {region_name} (시/도만 매칭)")

        region_info = f"[{region_code or '??'}/{sub_region_code or '????'} {region_name or ''} {sub_region_name or ''}]".strip()
        log(f"[idx={idx}] ▶  {keyword} (max={max_items}) {region_info}")

        success, rows, err = run_one_keyword(keyword, max_items)

        if success:
            try:
                result = upload_rows_to_firestore(
                    rows,
                    keyword,
                    region_code=region_code,
                    region_name=region_name,
                    sub_region_code=sub_region_code,
                    sub_region_name=sub_region_name,
                )
                mark_job_done(
                    keyword,
                    result["uploaded"],
                    result["place_ids"],
                    region_code=region_code,
                    sub_region_code=sub_region_code,
                )
                log(f"  ✅ Firestore: {result['uploaded']}건 (skip {result['skipped']})")
                succeeded += 1

                if SAVE_XLSX:
                    filename = keyword.replace(" ", "_")
                    xlsx_path = os.path.join(OUTPUT_DIR, f"{filename}.xlsx")
                    try:
                        save_excel_with_region(
                            rows, xlsx_path,
                            region_code, region_name,
                            sub_region_code, sub_region_name,
                        )
                    except Exception as e:
                        log(f"  ⚠  xlsx 저장 실패(무시): {e}")
            except Exception as e:
                log(f"  ❌ Firestore 업로드 실패: {e}")
                mark_job_failed(keyword, f"firestore upload: {e}", MAX_RETRIES)
                failed += 1
        else:
            log(f"  ❌ 크롤링 실패: {err[:200]}")
            mark_job_failed(keyword, err, MAX_RETRIES)
            failed += 1

        remaining = MAX_RUNTIME_SEC - (time.time() - started)
        if remaining > SLEEP_SEC:
            time.sleep(SLEEP_SEC)

    log(f"종료 — 처리:{processed}, 성공:{succeeded}, 실패:{failed}, 스킵:{skipped}")
    log(f"최종 last_claimed_index: {get_progress().get('last_claimed_index')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
