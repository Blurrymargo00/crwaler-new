"""
네이버 지도 크롤러
==================

네이버 지도(map.naver.com) 검색 결과를 수집해 엑셀 파일로 저장합니다.

동작 방식:
    GraphQL 직접 호출 대신, 네이버 프론트엔드가 실제로 쓰는 pcmap 페이지의
    embedded `window.__APOLLO_STATE__` JSON 을 파싱합니다.
      - 목록:  https://pcmap.place.naver.com/place/list?query=...&page=N
      - 상세:  https://pcmap.place.naver.com/place/{id}/home
    프론트가 살아있는 한 이 상태값은 항상 존재하고, 어떤 Apollo 타입을 쓰든
    우리가 필요한 필드는 그 안에 들어 있습니다. GraphQL 스키마 변동에 둔감합니다.

실행 예시:
    python naver_map_crawler.py "파주 운정 하수구"
    python naver_map_crawler.py "파주 운정 하수구" --max 60 --out result.xlsx
    python naver_map_crawler.py "파주 운정 하수구" --no-detail

필요 패키지:
    pip install requests pandas openpyxl
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Any
from urllib.parse import quote

import pandas as pd
import requests

# macOS / 일부 환경에서 stdout 이 ASCII/CP949 로 잡혀있을 때 한글이 '?' 로
# 깨지는 걸 방지
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
except (AttributeError, Exception):
    pass


# ---------------------------------------------------------------------------
# 상수
# ---------------------------------------------------------------------------

KST = timezone(timedelta(hours=9))

LIST_URL = "https://pcmap.place.naver.com/place/list"
PLACE_HOME_URL = "https://pcmap.place.naver.com/place/{place_id}/home"
PLACE_INFO_URL = "https://pcmap.place.naver.com/place/{place_id}/information"
PLACE_VISITOR_REVIEW_URL = "https://pcmap.place.naver.com/place/{place_id}/review/visitor"
PLACE_BLOG_REVIEW_URL = "https://pcmap.place.naver.com/place/{place_id}/review/ugc"
RESTAURANT_HOME_URL = "https://pcmap.place.naver.com/restaurant/{place_id}/home"
RESTAURANT_INFO_URL = "https://pcmap.place.naver.com/restaurant/{place_id}/information"
RESTAURANT_VISITOR_REVIEW_URL = "https://pcmap.place.naver.com/restaurant/{place_id}/review/visitor"
RESTAURANT_BLOG_REVIEW_URL = "https://pcmap.place.naver.com/restaurant/{place_id}/review/ugc"

# 엑셀 컬럼 순서 (업로드된 샘플 파일과 동일)
EXCEL_COLUMNS = [
    "이름", "카테고리", "도로명주소", "지번주소",
    "일반전화", "안심번호", "영업시간",
    "썸네일이미지URL",
    "방문자 리뷰수", "방문자 평점", "블로그 리뷰수",
    "위도", "경도",
    "매장정보", "부가설명",
    "홈페이지URL", "사진리뷰수", "상세페이지URL",
    "해시태그", "전화번호", "created_at",
]

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://map.naver.com/",
    "Upgrade-Insecure-Requests": "1",
}


# ---------------------------------------------------------------------------
# 데이터 모델
# ---------------------------------------------------------------------------

@dataclass
class PlaceRow:
    이름: str = ""
    카테고리: str = ""
    도로명주소: str = ""
    지번주소: str = ""
    일반전화: str = ""
    안심번호: str = ""
    영업시간: str = ""
    썸네일이미지URL: str = ""
    방문자_리뷰수: float | None = None
    방문자_평점: float | None = None
    블로그_리뷰수: float | None = None
    위도: float | None = None
    경도: float | None = None
    매장정보: str = ""
    부가설명: str = ""
    홈페이지URL: str = ""
    사진리뷰수: float | None = None
    상세페이지URL: str = ""
    해시태그: str = ""
    전화번호: str = ""
    created_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        raw = asdict(self)
        return {
            "이름": raw["이름"],
            "카테고리": raw["카테고리"],
            "도로명주소": raw["도로명주소"],
            "지번주소": raw["지번주소"],
            "일반전화": raw["일반전화"],
            "안심번호": raw["안심번호"],
            "영업시간": raw["영업시간"],
            "썸네일이미지URL": raw["썸네일이미지URL"],
            "방문자 리뷰수": raw["방문자_리뷰수"],
            "방문자 평점": raw["방문자_평점"],
            "블로그 리뷰수": raw["블로그_리뷰수"],
            "위도": raw["위도"],
            "경도": raw["경도"],
            "매장정보": raw["매장정보"],
            "부가설명": raw["부가설명"],
            "홈페이지URL": raw["홈페이지URL"],
            "사진리뷰수": raw["사진리뷰수"],
            "상세페이지URL": raw["상세페이지URL"],
            "해시태그": raw["해시태그"],
            "전화번호": raw["전화번호"],
            "created_at": raw["created_at"],
        }


# ---------------------------------------------------------------------------
# 세션
# ---------------------------------------------------------------------------

def make_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)
    # 쿠키 웜업
    try:
        s.get("https://pcmap.place.naver.com/", timeout=10)
    except requests.RequestException:
        pass
    return s


def polite_sleep(min_s: float = 0.4, max_s: float = 1.2) -> None:
    time.sleep(random.uniform(min_s, max_s))


# ---------------------------------------------------------------------------
# Apollo state 추출
# ---------------------------------------------------------------------------

def _match_json_object(text: str, start: int) -> str | None:
    """text[start] 가 '{' 라고 가정하고 균형 맞춰 JSON object 끝까지 반환."""
    if start >= len(text) or text[start] != "{":
        return None
    depth = 0
    in_str = False
    esc = False
    for i in range(start, len(text)):
        c = text[i]
        if esc:
            esc = False
            continue
        if in_str:
            if c == "\\":
                esc = True
            elif c == '"':
                in_str = False
            continue
        if c == '"':
            in_str = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    return None


def extract_apollo_state(html: str) -> dict[str, Any]:
    """<script> 블록 안의 window.__APOLLO_STATE__ = {...}; 를 추출."""
    for pattern in (
        r"window\.__APOLLO_STATE__\s*=\s*",
        r"__APOLLO_STATE__\s*=\s*",
    ):
        for m in re.finditer(pattern, html):
            start = m.end()
            while start < len(html) and html[start] != "{":
                start += 1
            if start >= len(html):
                continue
            blob = _match_json_object(html, start)
            if blob is None:
                continue
            try:
                return json.loads(blob)
            except json.JSONDecodeError:
                continue

    # 폴백: Next.js 스타일
    m = re.search(
        r'<script\s+id="__NEXT_DATA__"[^>]*>\s*(\{.*?\})\s*</script>',
        html,
        re.DOTALL,
    )
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    return {}


# ---------------------------------------------------------------------------
# 목록 / 상세 수집
# ---------------------------------------------------------------------------

LIST_TYPE_PREFIXES = (
    "PlaceSummary:",
    "RestaurantListSummary:",
    "HairshopSummary:",
    "AccommodationSummary:",
    "BeautySummary:",
    "HospitalSummary:",
    "AcademySummary:",
)

DETAIL_BASE_PREFIXES = (
    "PlaceDetailBase:",
    "RestaurantBase:",
    "HairshopBase:",
    "AccommodationBase:",
    "BeautyBase:",
    "HospitalBase:",
    "AcademyBase:",
)


def _decoded_html(r: requests.Response) -> str:
    """네이버 pcmap 은 Content-Type 에 charset 을 생략할 때가 많아
    requests 가 ISO-8859-1 로 잘못 디코딩하는 문제를 막는다.
    """
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "charset=" in ctype:
        return r.text
    # 본문 바이트를 UTF-8 로 강제 디코딩 (네이버는 UTF-8 로 서빙)
    try:
        return r.content.decode("utf-8")
    except UnicodeDecodeError:
        r.encoding = r.apparent_encoding or "utf-8"
        return r.text


def fetch_list_page(
    session: requests.Session,
    query: str,
    page: int,
) -> tuple[list[dict[str, Any]], int]:
    """목록 HTML → Apollo state → PlaceSummary 객체들."""
    params = {
        "query": query,
        "page": str(page),
        # display/size 는 서버가 무시하는 경우가 많음 (페이지당 고정)
    }
    r = session.get(LIST_URL, params=params, timeout=15)
    r.raise_for_status()
    state = extract_apollo_state(_decoded_html(r))

    items: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for key, val in state.items():
        if not isinstance(key, str) or not isinstance(val, dict):
            continue
        if any(key.startswith(p) for p in LIST_TYPE_PREFIXES):
            pid = str(val.get("id") or key.split(":", 1)[1])
            if pid in seen_ids:
                continue
            seen_ids.add(pid)
            items.append(val)
    return items, len(items)


def _fetch_apollo(session: requests.Session, url: str) -> tuple[dict[str, Any], str]:
    """URL 호출 후 (Apollo state, 원본 HTML) 을 반환. 실패 시 ({}, '')."""
    try:
        r = session.get(url, timeout=15)
        if r.status_code == 200:
            html = _decoded_html(r)
            return (extract_apollo_state(html) or {}, html)
    except requests.RequestException:
        pass
    return ({}, "")


def _merge_state(dst: dict[str, Any], src: dict[str, Any]) -> dict[str, Any]:
    """dst 에 src 를 병합. 같은 키 dict 면 내부 필드도 보존."""
    for k, v in src.items():
        if k in dst and isinstance(dst[k], dict) and isinstance(v, dict):
            joined = dict(dst[k])
            for kk, vv in v.items():
                if vv not in (None, "", []):
                    joined[kk] = vv
            dst[k] = joined
        else:
            dst[k] = v
    return dst


def fetch_detail_page(session: requests.Session, place_id: str) -> dict[str, Any]:
    """4개 탭(home/information/review.visitor/review.ugc) 페이지를 모두 받아
    Apollo state 를 병합하고, 각 탭 HTML 을 따로 보관한다.
      - home: 기본정보·영업시간
      - information: '소개' 본문·편의시설·og:description
      - review/visitor: 방문자 리뷰 카운트·평점
      - review/ugc: 블로그 리뷰 카운트
    """
    tab_urls: dict[str, tuple[str, ...]] = {
        "home": (
            PLACE_HOME_URL.format(place_id=place_id),
            RESTAURANT_HOME_URL.format(place_id=place_id),
        ),
        "info": (
            PLACE_INFO_URL.format(place_id=place_id),
            RESTAURANT_INFO_URL.format(place_id=place_id),
        ),
        "visitor_review": (
            PLACE_VISITOR_REVIEW_URL.format(place_id=place_id),
            RESTAURANT_VISITOR_REVIEW_URL.format(place_id=place_id),
        ),
        "blog_review": (
            PLACE_BLOG_REVIEW_URL.format(place_id=place_id),
            RESTAURANT_BLOG_REVIEW_URL.format(place_id=place_id),
        ),
    }

    merged: dict[str, Any] = {}
    html_by_tab: dict[str, str] = {}
    for tab_name, urls in tab_urls.items():
        tab_state: dict[str, Any] = {}
        tab_html: str = ""
        for url in urls:
            tab_state, tab_html = _fetch_apollo(session, url)
            if tab_state or tab_html:
                break
        html_by_tab[tab_name] = tab_html or ""
        if tab_state:
            _merge_state(merged, tab_state)
        polite_sleep(0.1, 0.3)

    if not merged and not any(html_by_tab.values()):
        return {}

    # base 후보: 필드가 가장 많은 것
    base: dict[str, Any] = {}
    stats: dict[str, Any] = {}
    for key, val in merged.items():
        if not isinstance(val, dict):
            continue
        if isinstance(key, str) and any(key.startswith(p) for p in DETAIL_BASE_PREFIXES):
            if len(val) > len(base):
                base = val
        typename = val.get("__typename", "")
        if isinstance(typename, str) and (
            typename.startswith("PlaceDetailStatistics")
            or typename.endswith("Statistics")
        ):
            stats = val

    joined_html = "\n".join(v for v in html_by_tab.values() if v)
    return {
        "base": base,
        "statistics": stats,
        "state": merged,
        "html": joined_html,
        "html_by_tab": html_by_tab,
    }


# ---------------------------------------------------------------------------
# 파싱 유틸
# ---------------------------------------------------------------------------

def as_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def clean_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (list, tuple)):
        return ", ".join(clean_text(x) for x in v if x)
    if isinstance(v, dict):
        # 흔한 래퍼: {"__ref": "Foo:123"} 같은 참조 객체는 빈 값 취급
        if set(v.keys()) == {"__ref"} or set(v.keys()) == {"__typename"}:
            return ""
        return ", ".join(f"{k}:{clean_text(val)}" for k, val in v.items() if val)
    return str(v).strip()


def resolve_ref(state: dict[str, Any], node: Any) -> Any:
    """Apollo cache 는 참조를 `{"__ref": "Typename:id"}` 로 저장한다. 풀어준다."""
    if isinstance(node, dict) and "__ref" in node and len(node) == 1:
        return state.get(node["__ref"], node)
    return node


def first_image(*candidates: Any) -> str:
    for c in candidates:
        if not c:
            continue
        if isinstance(c, str):
            return c
        if isinstance(c, list):
            for item in c:
                if isinstance(item, str) and item:
                    return item
                if isinstance(item, dict):
                    for k in ("url", "imageUrl", "src"):
                        if item.get(k):
                            return str(item[k])
    return ""


def format_hours(v: Any, state: dict[str, Any] | None = None) -> str:
    """NewBusinessHours 등 다양한 영업시간 스키마를 사람이 읽을 형태로 변환."""
    if v is None:
        return ""
    if state is not None:
        v = resolve_ref(state, v)
    if isinstance(v, str):
        return v.strip()

    if isinstance(v, dict):
        # NewBusinessHours: {status, description, businessHours?, businessHoursList?}
        parts: list[str] = []
        for key in ("status", "description", "summary", "text"):
            s = v.get(key)
            if isinstance(s, str) and s.strip():
                parts.append(s.strip())
        # 요일별 상세가 있으면 덧붙이기
        for key in ("businessHours", "newBusinessHours", "businessHoursList",
                     "weekScheduleList", "bizHourList"):
            inner = v.get(key)
            if inner:
                inner = resolve_ref(state or {}, inner)
                sub = format_hours(inner, state)
                if sub:
                    parts.append(sub)
        return " ".join(p for p in parts if p).strip()

    if isinstance(v, list):
        parts = []
        for it in v:
            it = resolve_ref(state or {}, it)
            if isinstance(it, dict):
                day = clean_text(it.get("day") or it.get("dayOfWeek"))
                hours = clean_text(
                    it.get("businessHours")
                    or it.get("description")
                    or it.get("text")
                    or it.get("timeRange")
                )
                chunk = f"{day} {hours}".strip()
                if chunk:
                    parts.append(chunk)
            else:
                s = clean_text(it)
                if s:
                    parts.append(s)
        return ", ".join(parts)

    return str(v)


def iter_nodes(root: Any) -> Any:
    """state 내부의 모든 dict 노드를 재귀 순회."""
    stack = [root]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            yield node
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)


def find_numeric_in_state(
    state: dict[str, Any],
    field_candidates: tuple[str, ...],
    typename_hints: tuple[str, ...] = (),
) -> float | None:
    """state 전체를 스캔해 숫자형 필드를 찾는다. typename_hints 가 주어지면
    해당 __typename 을 가진 노드를 우선 검사."""
    fallback: float | None = None
    for node in iter_nodes(state):
        if not isinstance(node, dict):
            continue
        typename = str(node.get("__typename") or "")
        priority = any(h in typename for h in typename_hints) if typename_hints else True
        for f in field_candidates:
            if f not in node:
                continue
            v = node[f]
            if v is None:
                continue
            try:
                if isinstance(v, str):
                    v = v.replace(",", "").strip()
                    if not v:
                        continue
                num = float(v)
            except (TypeError, ValueError):
                continue
            if priority:
                return num
            fallback = fallback if fallback is not None else num
    return fallback


def _coerce_number(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.replace(",", "").strip()
        if not s:
            return None
        # 예: "2,777+", "2.7천" 같은 표기 회피 — 순수 정수/소수만 수용
        m = re.match(r"^-?\d+(?:\.\d+)?$", s)
        if m:
            try:
                return float(s)
            except ValueError:
                return None
    return None


_COUNT_HINTS = ("count", "total", "cnt", "reviews", "num")
_COUNT_EXCLUDES = (
    "page", "index", "offset", "limit", "size", "perpage", "pagesize",
    "id", "url", "link", "thumb", "image", "marker",
)


def find_count_by_key_substr(
    state: dict[str, Any],
    key_substr: str,
    *,
    require_count_hint: bool = True,
    extra_excludes: tuple[str, ...] = (),
) -> float | None:
    """키 이름에 key_substr 이 포함된 숫자 필드 중 카운트 힌트
    (count/total/cnt/reviews/num) 가 함께 들어간 것만 채택해 최대값 반환.
    페이지·인덱스·리미트처럼 카운트가 아닌 필드는 제외한다.
    """
    key_substr = key_substr.lower()
    excludes = _COUNT_EXCLUDES + tuple(s.lower() for s in extra_excludes)
    candidates: list[float] = []
    for node in iter_nodes(state):
        if not isinstance(node, dict):
            continue
        for k, v in node.items():
            if not isinstance(k, str):
                continue
            kl = k.lower()
            if key_substr not in kl:
                continue
            if any(x in kl for x in excludes):
                continue
            if require_count_hint and not any(h in kl for h in _COUNT_HINTS):
                continue
            num = _coerce_number(v)
            if num is not None:
                candidates.append(num)
    return max(candidates) if candidates else None


_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _html_to_text(html: str) -> str:
    """태그 제거 후 공백 정규화. 패턴 매칭을 단순화하려는 용도."""
    if not html:
        return ""
    # <script>/<style> 블록은 통째로 제거 (스크립트 안의 라벨 텍스트 오탐 방지)
    cleaned = re.sub(r"<(script|style)\b[^>]*>[\s\S]*?</\1>", " ", html, flags=re.IGNORECASE)
    cleaned = _TAG_RE.sub(" ", cleaned)
    cleaned = cleaned.replace("&nbsp;", " ")
    cleaned = _WS_RE.sub(" ", cleaned)
    return cleaned


_BLOG_HTML_PATTERN = re.compile(r"블로그\s*리뷰\s*(?:수|개|건)?\s*([\d,]+)")
_VISITOR_HTML_PATTERN = re.compile(r"방문자\s*리뷰\s*(?:수|개|건)?\s*([\d,]+)")
_PHOTO_HTML_PATTERN = re.compile(r"사진\s*리뷰\s*(?:수|개|건)?\s*([\d,]+)")

_COUNT_PATTERN_BY_KIND = {
    "visitor": _VISITOR_HTML_PATTERN,
    "blog": _BLOG_HTML_PATTERN,
    "photo": _PHOTO_HTML_PATTERN,
}


def _count_by_kind(html: str, kind: str) -> float | None:
    """HTML 에서 '방문자 리뷰 N' / '블로그 리뷰 N' 같은 **명시 라벨** 카운트 추출.

    네이버 pcmap 리뷰 탭에는 항상 두 sub-tab 라벨
    (예: "방문자 리뷰 2", "블로그 리뷰 0") 이 배지 형태로 함께 노출된다.
    따라서 한 탭 HTML 에서 두 종류 카운트를 모두 안전하게 뽑을 수 있다.

    bare "리뷰 N" 패턴은 쓰지 않는다:
      - 유저 프로필에 '리뷰 2,329' 처럼 유저가 쓴 총 리뷰 수 배지가 있고,
      - 블로그 탭이 redirect 돼서 방문자 탭 HTML 이 내려오는 케이스에서
        이 프로필 배지를 블로그 카운트로 잘못 집어오는 문제가 있었다.
    """
    if not html:
        return None
    pat = _COUNT_PATTERN_BY_KIND.get(kind)
    if pat is None:
        return None
    text = _html_to_text(html)
    m = pat.search(text)
    if not m:
        return None
    s = m.group(1).replace(",", "")
    return float(int(s)) if s.isdigit() else None


def _count_from_multiple(html_parts: list[str], kind: str) -> float | None:
    """여러 탭 HTML 을 순회하며 첫 번째로 라벨 매치에 성공하는 카운트 반환.
    리뷰 탭이 redirect 되거나 없는 케이스(블로그 리뷰 0 → 탭 자체가 사라짐)에
    대비해 방문자/홈/정보 탭 HTML 도 함께 스캔한다.
    """
    for h in html_parts:
        v = _count_by_kind(h, kind)
        if v is not None:
            return v
    return None


def _og_description(html: str) -> str:
    """meta[property=og:description] 의 content 를 반환."""
    if not html:
        return ""
    import html as htmllib  # 로컬 임포트로 모듈 이름 충돌 회피
    for pat in (
        r'<meta\s+property=["\']og:description["\']\s+content=["\']([^"\']+)["\']',
        r'<meta\s+content=["\']([^"\']+)["\']\s+property=["\']og:description["\']',
        r'<meta\s+name=["\']description["\']\s+content=["\']([^"\']+)["\']',
    ):
        m = re.search(pat, html, re.IGNORECASE)
        if m:
            return htmllib.unescape(m.group(1)).strip()
    return ""


# 정보탭에서 '소개' 섹션 뒤에 나올 수 있는 '다른 섹션' 제목들.
# 소개 본문이 이런 제목에 도달하면 거기서 끊어준다.
_INTRO_SECTION_ENDS = (
    "편의", "편의시설", "주차", "메뉴", "가격", "예약", "결제",
    "영업시간", "전화", "주소", "찾아오는길", "위치",
    "상세정보", "사업자", "서비스", "최근", "대표키워드",
    "정보수정", "업체정보", "홈페이지", "인스타", "블로그",
)


def _introduction_from_info_html(html: str) -> str:
    """정보탭 HTML 에서 '소개' 섹션 본문을 텍스트로 추출.

    네이버 pcmap 은 클래스명을 자주 바꾸므로, 문자열 '소개' 가 heading
    콘텍스트(`>소개<`)에 등장하는 지점을 앵커로 잡고, 그 뒤의 HTML 을
    텍스트로 정규화한 다음 '다음 섹션' 제목이 나오면 그 앞에서 끊는다.
    """
    if not html:
        return ""
    # '>소개<...>' 매치. 헤더 태그 안의 '소개' 텍스트를 모두 커버하고,
    # 닫는 태그(`</h3>` 등) 까지 consume 해서 잔재가 본문에 새지 않게 한다.
    m = re.search(r">\s*소개\s*<[^>]*>", html)
    if not m:
        return ""
    tail_html = html[m.end():]
    # script/style 잔재 제거
    tail_html = re.sub(
        r"<(script|style)\b[^>]*>[\s\S]*?</\1>", " ", tail_html, flags=re.IGNORECASE
    )
    # 본문을 텍스트로 내린 뒤 너무 길면 5000자로 컷
    text = _html_to_text(tail_html).strip()
    if len(text) > 5000:
        text = text[:5000]
    # 앞쪽에 남은 '소개' 헤더 잔재 제거
    text = re.sub(r"^소개\s*", "", text).strip()
    # 다음 섹션 헤더 문자열을 찾아 그 앞까지만 남김 (가장 가까운 것 우선)
    cut_at: int | None = None
    for keyword in _INTRO_SECTION_ENDS:
        idx = text.find(keyword)
        if idx >= 0:
            # 너무 앞쪽(20자 미만)이면 오탐 가능성 높으니 스킵
            if idx < 20:
                continue
            if cut_at is None or idx < cut_at:
                cut_at = idx
    if cut_at is not None:
        text = text[:cut_at].strip()
    # UI 버튼/잔재 제거
    text = re.sub(
        r"\b(더보기|접기|펼치기|자세히\s*보기|정보\s*수정\s*제안|사업자\s*정보\s*수정)\b",
        "",
        text,
    ).strip()
    # 공백 정규화
    text = _WS_RE.sub(" ", text).strip()
    return text


def _match_count(pattern: re.Pattern, html: str) -> float | None:
    if not html:
        return None
    text = _html_to_text(html)
    m = pattern.search(text)
    if m:
        s = m.group(1).replace(",", "").strip()
        if s.isdigit():
            return float(s)
    return None


def find_blog_reviews(
    state: dict[str, Any],
    html: str = "",
    blog_tab_html: str = "",
) -> float | None:
    # 1) 블로그 리뷰 탭 HTML — 가장 신뢰도 높음 ("블로그 리뷰 2,091" 라벨 직접 표시)
    v = _count_by_kind(blog_tab_html, "blog")
    if v is not None:
        return v
    # 2) state 의 blog*count / blog*total / blog*reviews 숫자 (엄격 매칭)
    v = find_count_by_key_substr(state, "blog")
    if v is not None:
        return v
    v = find_count_by_key_substr(state, "blogcafe")
    if v is not None:
        return v
    # 3) 병합 HTML 본문 정규식 폴백
    return _match_count(_BLOG_HTML_PATTERN, html)


def find_visitor_reviews(
    state: dict[str, Any],
    html: str = "",
    visitor_tab_html: str = "",
) -> float | None:
    # 1) 방문자 리뷰 탭 HTML
    v = _count_by_kind(visitor_tab_html, "visitor")
    if v is not None:
        return v
    # 2) state 의 명시적 필드
    v = find_numeric_in_state(
        state,
        ("visitorReviewsTotal", "visitorReviewCount", "totalVisitorReviewCount",
         "visitorReviewTotal"),
        ("Statistics", "Review", "Visitor"),
    )
    if v is not None:
        return v
    # 3) 키에 visitor+review+count 힌트 (엄격)
    for node in iter_nodes(state):
        if not isinstance(node, dict):
            continue
        for k, val in node.items():
            if not isinstance(k, str):
                continue
            kl = k.lower()
            if "visitor" in kl and "review" in kl and "score" not in kl:
                if any(h in kl for h in _COUNT_HINTS):
                    num = _coerce_number(val)
                    if num is not None:
                        return num
    return _match_count(_VISITOR_HTML_PATTERN, html)


def find_visitor_score(state: dict[str, Any]) -> float | None:
    v = find_numeric_in_state(
        state,
        ("visitorReviewsScore", "visitorReviewScore", "averageScore", "scoreAverage",
         "score"),
        ("Statistics", "Review"),
    )
    if v is not None and 0 <= v <= 10:
        return v
    # score 키 + review/visitor 힌트
    for node in iter_nodes(state):
        if not isinstance(node, dict):
            continue
        typename = str(node.get("__typename") or "").lower()
        if "review" not in typename and "statistics" not in typename and "rating" not in typename:
            continue
        for k, val in node.items():
            if not isinstance(k, str):
                continue
            if "score" in k.lower() or "rating" in k.lower() or "average" in k.lower():
                num = _coerce_number(val)
                if num is not None and 0 <= num <= 10:
                    return num
    return None


def find_photo_reviews(state: dict[str, Any], html: str = "") -> float | None:
    v = find_numeric_in_state(
        state,
        ("photoReviewsTotal", "photoReviewCount", "totalPhotoReviewCount"),
        ("Statistics", "Review", "Photo"),
    )
    if v is not None:
        return v
    for node in iter_nodes(state):
        if not isinstance(node, dict):
            continue
        for k, val in node.items():
            if not isinstance(k, str):
                continue
            kl = k.lower()
            if "photo" in kl and ("review" in kl or "count" in kl or "total" in kl):
                num = _coerce_number(val)
                if num is not None:
                    return num
    return _match_count(_PHOTO_HTML_PATTERN, html)


def _resolve_text_field(state: dict[str, Any], v: Any) -> str:
    """Apollo cache 에선 긴 텍스트가 `{"__ref": "IntroBlock:123"}` 로 normalize 되기도 한다.
    ref 를 풀고, 해소된 노드에서 text/content/description 을 꺼낸다.
    """
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, dict):
        # {__ref: "Foo:123"} 형태
        if set(v.keys()) == {"__ref"}:
            referenced = state.get(v["__ref"])
            if isinstance(referenced, dict):
                for tf in ("text", "content", "description", "value", "body"):
                    tv = referenced.get(tf)
                    if isinstance(tv, str) and tv.strip():
                        return tv.strip()
            return ""
        # 직접 text/content 필드를 품고 있는 경우
        for tf in ("text", "content", "description", "value", "body"):
            tv = v.get(tf)
            if isinstance(tv, str) and tv.strip():
                return tv.strip()
    return ""


def find_introduction(state: dict[str, Any]) -> str:
    """정보탭의 '소개' 본문을 state 에서 탐색.

    전략
      1) state 어디든 `introduction` / `bizIntroduction` 등 소개 전용 필드가
         있으면 (Apollo __ref 까지 해소해서) 가장 긴 것을 채택.
      2) 못 찾으면 소개 관련 타입네임 노드의 text/description/content 중
         가장 긴 것. Parking, Amenity, Menu, Option, Hour, Review 등 다른
         섹션 타입네임은 배제.
    """
    # 1단계: 소개 전용 필드명 (후보 대폭 확장 + __ref 해소)
    intro_field_names = (
        "introduction", "bizIntroduction", "introductionText",
        "ownerIntroduction", "introductionContent",
        "bizIntro", "placeIntroduction", "placeIntroText",
        "ownerReviewContent", "ownerReview",
        "storeIntroduction", "storeIntro",
        "shortDescription", "detailInfo",
    )
    best = ""
    for node in iter_nodes(state):
        if not isinstance(node, dict):
            continue
        for f in intro_field_names:
            if f not in node:
                continue
            s = _resolve_text_field(state, node[f])
            if s and len(s) > len(best):
                best = s
    if best:
        return best

    # 2단계: 타입네임 기반 폴백
    positive_hints = (
        "Intro", "Introduction", "BizProfile", "PlaceProfile", "OwnerReview",
        "BizDetail", "BizSection", "BizOverview", "Profile", "Owner",
        "StoreIntro", "StoreProfile", "PlaceDescription",
    )
    # 아래 섹션 타입네임은 소개가 아님
    negative_hints = (
        "Parking", "Amenity", "Menu", "Option", "Feature",
        "Convenience", "Service", "Facility", "Hour", "Schedule",
        "Review", "Statistics", "Marker", "Summary", "Photo",
        "Coupon", "Event", "News", "Booking", "Reservation",
    )
    for node in iter_nodes(state):
        if not isinstance(node, dict):
            continue
        typename = str(node.get("__typename") or "")
        if not typename:
            continue
        if any(n in typename for n in negative_hints):
            continue
        if not any(p in typename for p in positive_hints):
            continue
        for f in ("description", "content", "text", "summary", "body"):
            v = node.get(f)
            s = _resolve_text_field(state, v)
            if s and len(s) > len(best):
                best = s
    return best


def pick_homepage(v: Any) -> str:
    if not v:
        return ""
    if isinstance(v, dict):
        for k in ("repr", "etc", "url", "homepage"):
            if v.get(k):
                return clean_text(v[k])
        return clean_text(v)
    if isinstance(v, list):
        urls = [clean_text(x) for x in v if x]
        return ", ".join(u for u in urls if u)
    return clean_text(v)


# ---------------------------------------------------------------------------
# 행 빌드
# ---------------------------------------------------------------------------

def build_row(item: dict[str, Any], detail: dict[str, Any] | None, query: str) -> PlaceRow:
    base = (detail or {}).get("base") or {}
    stats = (detail or {}).get("statistics") or {}
    state = (detail or {}).get("state") or {}

    # 목록과 상세 값을 병합한 view. 상세값이 우선.
    def pick(*keys: str) -> Any:
        for k in keys:
            if base.get(k) not in (None, ""):
                return base[k]
            if item.get(k) not in (None, ""):
                return item[k]
        return None

    name = clean_text(pick("name"))
    category = clean_text(pick("category", "businessCategory"))
    road_addr = clean_text(pick("roadAddress"))
    addr = clean_text(pick("address", "commonAddress"))
    phone = clean_text(pick("phone"))
    virt_phone = clean_text(pick("virtualPhone"))
    biz_hours = format_hours(pick("bizhourInfo", "newBusinessHours", "businessHours"), state)
    thumb = first_image(
        base.get("thumUrls"), item.get("thumUrls"),
        base.get("imageUrls"), item.get("imageUrls"),
        base.get("thumUrl"), item.get("thumUrl"),
        base.get("imageUrl"), item.get("imageUrl"),
    )

    lat = as_float(pick("y"))
    lng = as_float(pick("x"))

    def first_present(*sources: dict[str, Any], key: str) -> Any:
        """0 이나 0.0 도 유효값으로 보존. None/빈문자열 만 스킵."""
        for src in sources:
            if not isinstance(src, dict):
                continue
            v = src.get(key)
            if v is not None and v != "":
                return v
        return None

    # 1차: stats/base/item 의 표준 필드
    visitor = as_float(first_present(stats, base, item, key="visitorReviewsTotal"))
    score = as_float(first_present(stats, base, item, key="visitorReviewsScore"))
    blog = as_float(first_present(stats, base, item, key="blogCafeReviewsTotal"))
    photo = as_float(first_present(stats, base, key="photoReviewsTotal"))

    # 2차: state 전체 + 탭별 HTML 폴백
    html_blob = (detail or {}).get("html") or ""
    html_by_tab = (detail or {}).get("html_by_tab") or {}
    visitor_tab_html = html_by_tab.get("visitor_review", "")
    blog_tab_html = html_by_tab.get("blog_review", "")
    home_tab_html = html_by_tab.get("home", "")
    info_tab_html = html_by_tab.get("info", "")

    # pcmap 리뷰 탭에는 "방문자 리뷰 2", "블로그 리뷰 0" 같은 sub-tab 라벨이
    # 배지 형태로 함께 렌더되므로, 한 탭 HTML 에서 두 카운트 모두를 뽑을 수 있다.
    # 따라서 블로그 탭이 redirect 되거나(블로그 리뷰 0인 케이스) 빈 HTML 이어도
    # 방문자 탭 HTML 에서 블로그 라벨을 찾을 수 있게 여러 탭을 순회한다.
    blog_from_tab = _count_from_multiple(
        [blog_tab_html, visitor_tab_html, home_tab_html, info_tab_html], "blog"
    )
    if blog_from_tab is not None:
        blog = blog_from_tab
    elif blog is None:
        blog = find_blog_reviews(state, html_blob, blog_tab_html)

    visitor_from_tab = _count_from_multiple(
        [visitor_tab_html, blog_tab_html, home_tab_html, info_tab_html], "visitor"
    )
    if visitor_from_tab is not None:
        visitor = visitor_from_tab
    elif visitor is None:
        visitor = find_visitor_reviews(state, html_blob, visitor_tab_html)

    if score is None:
        score = find_visitor_score(state)
    if photo is None:
        photo = find_photo_reviews(state, html_blob)

    amenities = clean_text(
        base.get("amenities")
        or base.get("options")
        or base.get("conveniences")
        or base.get("services")
    )
    # 부가설명 = 정보탭의 '소개' 본문.
    # 우선순위:
    #   1) state 의 introduction / bizIntroduction 전용 필드 (__ref 해소 포함)
    #   2) 정보탭 HTML 에서 "소개" 섹션 본문 파싱
    #   3) 홈탭 HTML 에서 "소개" 섹션 본문 파싱 (일부 업종은 홈에 노출)
    #   4) base.bizIntroduction / base.description
    #   5) og:description — 단, 카테고리·이름만 들어간 무의미 문자열은 배제
    description = find_introduction(state)
    if not description:
        description = _introduction_from_info_html(info_tab_html)
    if not description:
        description = _introduction_from_info_html(home_tab_html)
    if not description:
        description = clean_text(base.get("bizIntroduction") or base.get("description"))
    if not description:
        og = _og_description(info_tab_html or home_tab_html)
        # og:description 이 상호명/카테고리 콤보에 불과하면 (예: "싱크대막힘")
        # 의미있는 소개가 아니므로 버린다. 50자 미만 + 이름 단순 포함.
        if og and (len(og) >= 50 or (name and name not in og)):
            description = og

    hashtags = clean_text(base.get("hashTags") or base.get("tags") or item.get("tags"))
    homepage = pick_homepage(base.get("homepages") or base.get("homepage"))

    pid = str(base.get("id") or item.get("id") or "")
    detail_url = (
        f"https://map.naver.com/p/search/{quote(query)}/place/{pid}?c=13.00,0,0,0,dh"
        if pid
        else ""
    )

    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S %z")
    ts = re.sub(r"(\+\d{2})(\d{2})$", r"\1\2", ts)

    return PlaceRow(
        이름=name,
        카테고리=category,
        도로명주소=road_addr,
        지번주소=addr,
        일반전화=phone,
        안심번호=virt_phone,
        영업시간=biz_hours,
        썸네일이미지URL=thumb,
        방문자_리뷰수=visitor,
        방문자_평점=score,
        블로그_리뷰수=blog,
        위도=lat,
        경도=lng,
        매장정보=amenities,
        부가설명=description,
        홈페이지URL=homepage,
        사진리뷰수=photo,
        상세페이지URL=detail_url,
        해시태그=hashtags,
        전화번호=virt_phone or phone,
        created_at=ts,
    )


# ---------------------------------------------------------------------------
# 엑셀 저장
# ---------------------------------------------------------------------------

def save_excel(rows: list[PlaceRow], path: str) -> None:
    df = pd.DataFrame([r.to_dict() for r in rows], columns=EXCEL_COLUMNS)
    for col in ("방문자 리뷰수", "방문자 평점", "블로그 리뷰수", "사진리뷰수", "위도", "경도"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Sheet1")
        ws = writer.sheets["Sheet1"]
        for i, col in enumerate(df.columns, start=1):
            ws.column_dimensions[ws.cell(row=1, column=i).column_letter].width = max(
                12, min(40, len(col) + 4)
            )


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

def crawl(
    query: str,
    max_items: int,
    fetch_detail: bool = True,
    debug_dump: bool = False,
) -> list[PlaceRow]:
    session = make_session()

    all_items: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    page = 1
    while len(all_items) < max_items and page <= 20:
        try:
            items, got = fetch_list_page(session, query, page)
        except requests.RequestException as e:
            print(f"[list] page={page} 에러: {e}", file=sys.stderr)
            break
        if not items:
            break
        new_items = 0
        for it in items:
            pid = str(it.get("id") or "")
            if pid and pid not in seen_ids:
                seen_ids.add(pid)
                all_items.append(it)
                new_items += 1
                if len(all_items) >= max_items:
                    break
        print(f"[list] page={page} got={got} new={new_items} cum={len(all_items)}")
        if new_items == 0:
            break
        page += 1
        polite_sleep()

    all_items = all_items[:max_items]

    rows: list[PlaceRow] = []
    for idx, item in enumerate(all_items, start=1):
        detail: dict[str, Any] | None = None
        pid = str(item.get("id") or "")
        if fetch_detail and pid:
            try:
                detail = fetch_detail_page(session, pid)
                if debug_dump and idx == 1:
                    _dump_debug(pid, detail)
            except Exception as e:
                print(f"  [detail] id={pid} 실패: {e}")
            polite_sleep(0.3, 0.8)
        row = build_row(item, detail, query)
        rows.append(row)
        print(
            f"  [{idx}/{len(all_items)}] {row.이름 or pid}  ({row.카테고리})"
            f"  | 방문자:{row.방문자_리뷰수} 블로그:{row.블로그_리뷰수}"
        )
    return rows


def _dump_debug(place_id: str, detail: dict[str, Any] | None) -> None:
    """첫 장소의 state 와 HTML 을 파일로 떨궈서 디버깅에 활용."""
    if not detail:
        return
    ts = datetime.now(KST).strftime("%Y%m%d%H%M%S")
    state_path = f"debug_state_{place_id}_{ts}.json"
    html_path = f"debug_html_{place_id}_{ts}.html"
    try:
        with open(state_path, "w", encoding="utf-8") as f:
            json.dump(detail.get("state") or {}, f, ensure_ascii=False, indent=2)
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(detail.get("html") or "")
        print(f"  [debug] state → {state_path}, html → {html_path}")
    except OSError as e:
        print(f"  [debug] 덤프 실패: {e}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="네이버 지도 검색 결과 크롤러")
    p.add_argument("query", help='검색어 (예: "파주 운정 하수구")')
    p.add_argument("--max", type=int, default=50, help="최대 수집 개수 (기본 50)")
    p.add_argument(
        "--no-detail",
        action="store_true",
        help="상세 페이지 조회를 건너뛰어 속도를 높입니다",
    )
    p.add_argument(
        "--out",
        default=None,
        help="출력 엑셀 경로. 기본: 네이버_지도_수집_YYYYMMDDHHMMSS.xlsx",
    )
    p.add_argument(
        "--debug",
        action="store_true",
        help="첫 장소의 Apollo state 와 HTML 을 파일로 덤프 (필드명 점검용)",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    rows = crawl(
        args.query,
        args.max,
        fetch_detail=not args.no_detail,
        debug_dump=args.debug,
    )
    if not rows:
        print("수집 결과가 없습니다.", file=sys.stderr)
        return 1
    if args.out:
        out_path = args.out
    else:
        ts = datetime.now(KST).strftime("%Y%m%d%H%M%S")
        out_path = f"네이버_지도_수집_{ts}.xlsx"
    save_excel(rows, out_path)
    print(f"\n✅ 저장 완료: {out_path}  (총 {len(rows)}건)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
