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
import re
import sys
import time
from datetime import datetime

import pandas as pd

import naver_map_crawler as nmc
from naver_map_crawler import crawl, EXCEL_COLUMNS

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


# crawler_commands.txt 명령 형식:
#   python naver_map_crawler.py "서울 강남 입주청소" --max 20
CMD_RE = re.compile(r'"\s*([^"]+?)\s*"\s*(?:--max\s+(\d+))?')


def parse_command(cmd: str) -> tuple[str, int]:
    """명령 라인에서 (keyword, max_items) 추출."""
    m = CMD_RE.search(cmd)
    if not m:
        raise ValueError(f"명령 파싱 실패: {cmd}")
    keyword = m.group(1).strip()
    max_items = int(m.group(2)) if m.group(2) else 50
    return keyword, max_items


def run_one_keyword(keyword: str, max_items: int) -> tuple[bool, list, str]:
    """단일 키워드 크롤링 + 재시도.

    Returns: (success, rows, error_msg)
    """
    last_err = ""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            rows = crawl(keyword, max_items, fetch_detail=True, debug_dump=False)
            if not rows:
                last_err = "수집 결과 0건 (rate limit 또는 매칭 없음)"
            else:
                return True, rows, ""
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"

        if attempt >= MAX_RETRIES:
            break

        # 429 의심 시 더 긴 백오프
        if "429" in last_err or "결과 0건" in last_err:
            backoff = RETRY_BASE_SLEEP * (2 ** (attempt - 1))  # 180 → 360 → 720
        else:
            backoff = 60 * attempt

        log(f"  ⚠  실패(시도 {attempt}/{MAX_RETRIES}): {last_err[:200]}")
        log(f"  ⏳ {backoff}초 대기 후 재시도...")
        time.sleep(backoff)

    return False, [], last_err


def save_excel_with_region(
    rows: list,
    path: str,
    region_code: str | None,
    region_name: str | None,
    sub_region_code: str | None,
    sub_region_name: str | None,
) -> None:
    """크롤러의 save_excel 과 동일하되, region 컬럼 4개 추가.

    경기 동 단위(region_code 4자리): 지역코드=시군구코드, 지역명=시군구명, 시군구코드=읍면동코드, 시군구명=읍면동명
    일반: 지역코드=시도코드, 지역명=시도명, 시군구코드=시군구코드, 시군구명=시군구명
    """
    is_gyeonggi_leaf = region_code and len(region_code) == 4 and region_code.startswith("09")

    if is_gyeonggi_leaf:
        col_names = ["시군구코드", "시군구명", "읍면동코드", "읍면동명"]
    else:
        col_names = ["지역코드", "지역명", "시군구코드", "시군구명"]

    columns = EXCEL_COLUMNS + col_names

    records = []
    for r in rows:
        d = r.to_dict() if hasattr(r, "to_dict") else r
        d[col_names[0]] = region_code or ""
        d[col_names[1]] = region_name or ""
        d[col_names[2]] = sub_region_code or ""
        d[col_names[3]] = sub_region_name or ""
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

        # 경기 동 단위: region=시군구(4자리), sub_region=읍면동(6자리)
        # 일반: region=시도(2자리), sub_region=시군구(4자리)
        is_gyeonggi_leaf = region_code and len(region_code) == 4 and region_code.startswith("09")
        if is_gyeonggi_leaf:
            region_info = f"[경기/{region_name}/{sub_region_name or '??'}]"
        else:
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