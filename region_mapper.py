"""
region_mapper.py

키워드에서 시/도 + 시/군/구 + (옵션) 읍/면/동을 매칭해서 코드로 변환.

매칭 규칙:
  - 키워드 첫 토큰: 시/도 이름과 정확히 일치해야 함 ("서울", "경기", ...)
  - 키워드 두 번째 토큰: 시/군/구 이름과 매칭
      * 정확 일치 우선 ("강남구")
      * suffix(구/시/군) 떼고 비교 ("강남" → "강남구")
      * 동/읍/면 매칭 ("운정" → 파주시의 운정동)
  - 매칭 실패 시: (None, None, None, None) 반환

반환: (region_code, region_name, sub_region_code, sub_region_name)
"""
from __future__ import annotations

from typing import Optional
from regions import REGIONS


# ---------------------------------------------------------------------------
# 인덱스 빌드 (모듈 로드 시 1회)
# ---------------------------------------------------------------------------

# 시/도 이름 → top 객체
_TOP_BY_NAME: dict[str, dict] = {}

# (top_code, sub_name_정규화) → sub 객체  ← 첫 번째 hit 우선
_SUB_BY_TOP_NAME: dict[tuple[str, str], dict] = {}

# (top_code, sub_object) → 부모 sub 정보 (3-depth: 경기도용)
# (top_code, leaf_name) → (parent_sub_obj) — "운정" → 파주시 객체
_LEAF_TO_PARENT: dict[tuple[str, str], dict] = {}

# (top_code, leaf_name) → leaf 객체 — "운정" → {code: "092712", name: "운정동"}
_LEAF_BY_NAME: dict[tuple[str, str], dict] = {}


def _normalize_sub_name(name: str) -> str:
    """시/군/구 이름에서 suffix 제거. '강남구' → '강남', '파주시' → '파주', '가평군' → '가평'."""
    for suffix in ("특별시", "광역시", "특별자치시", "특별자치도", "시", "군", "구"):
        if name.endswith(suffix) and len(name) > len(suffix):
            return name[: -len(suffix)]
    return name


def _normalize_leaf_name(name: str) -> str:
    """읍/면/동 이름에서 suffix 제거. '운정동' → '운정', '곤지암읍' → '곤지암'."""
    for suffix in ("읍", "면", "동"):
        if name.endswith(suffix) and len(name) > len(suffix):
            return name[: -len(suffix)]
    return name


def _build_index() -> None:
    for top_code, top in REGIONS.items():
        _TOP_BY_NAME[top["name"]] = top
        for sub in top.get("subs", []):
            # 정확 일치 (강남구, 파주시 등)
            key_exact = (top_code, sub["name"])
            _SUB_BY_TOP_NAME.setdefault(key_exact, sub)
            # 정규화 일치 (강남, 파주 등)
            key_norm = (top_code, _normalize_sub_name(sub["name"]))
            _SUB_BY_TOP_NAME.setdefault(key_norm, sub)

            # 3-depth (경기도): 읍/면/동 → 부모 시/군 매핑 + leaf 자체 저장
            for leaf in sub.get("subs", []):
                leaf_key_exact = (top_code, leaf["name"])
                leaf_key_norm = (top_code, _normalize_leaf_name(leaf["name"]))
                _LEAF_TO_PARENT.setdefault(leaf_key_exact, sub)
                _LEAF_TO_PARENT.setdefault(leaf_key_norm, sub)
                _LEAF_BY_NAME.setdefault(leaf_key_exact, leaf)
                _LEAF_BY_NAME.setdefault(leaf_key_norm, leaf)


_build_index()


# ---------------------------------------------------------------------------
# 매핑 함수
# ---------------------------------------------------------------------------

def map_keyword_to_region(
    keyword: str,
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    "서울 강남 리모델링"      → ("01", "서울",  "0101",   "강남구")
    "경기 파주 교하 소독업체" → ("0927", "파주시", "092711", "교하동")  ← 경기: 1depth=시군구
    "파주 교하 소독업체"      → ("0927", "파주시", "092711", "교하동")  ← top 없이도 동일
    "광주 오포 입주청소"      → ("0905", "광주시", "090512", "오포동")  ← fallback + 경기 승격
    "경기 파주 청소"          → ("09",   "경기",   "0927",   "파주시")  ← leaf 없으면 기존대로
    매칭 실패 시 (None, None, None, None).
    경기도 leaf 매칭 시: region=시군구(4자리), sub_region=읍면동(6자리)
    그 외: region=시도(2자리), sub_region=시군구(4자리)
    """
    tokens = keyword.strip().split()
    if not tokens:
        return None, None, None, None

    # 1) 첫 토큰으로 시/도 찾기
    top = _TOP_BY_NAME.get(tokens[0])
    if top is None:
        # 첫 토큰이 시/도가 아닌 경우: "파주 운정 하수구" 처럼 시/군이 첫 토큰인 케이스
        # → 모든 시/도를 돌면서 sub 매칭 시도
        return _try_match_without_top(tokens)

    top_code = top["code"]
    top_name = top["name"]

    # 2) 두 번째 토큰으로 시/군/구 찾기
    if len(tokens) < 2:
        # 시/도만 있는 경우 — sub 가 1개뿐이면 자동 매핑 (세종 케이스)
        if len(top.get("subs", [])) == 1:
            sub = top["subs"][0]
            return top_code, top_name, sub["code"], sub["name"]
        return top_code, top_name, None, None

    sub = _find_sub(top_code, tokens[1])
    if sub is None:
        # sub 매칭 실패해도, sub 가 1개뿐이면 자동 매핑 (세종)
        if len(top.get("subs", [])) == 1:
            sub = top["subs"][0]
            return top_code, top_name, sub["code"], sub["name"]

        # ── fallback: 첫 토큰("광주")이 시/도 이름이기도 하지만 다른 시/도의
        #    sub("경기 광주시")이기도 한 경우.
        #    ex) "광주 오포 청소" → 광주광역시에서 "오포" 못 찾음
        #        → 경기도 광주시(sub)를 부모로 재시도
        fallback = _try_match_as_sub(tokens[0], tokens[1], exclude_top=top_code)
        if fallback != (None, None, None, None):
            return fallback

        return top_code, top_name, None, None

    # sub가 시군구(4자리)일 때, 세 번째 토큰으로 leaf(읍/면/동) 매칭 시도
    # ex) "경기 파주 교하 소독업체" → tokens[2]="교하" → 교하동(092711)
    if len(sub["code"]) == 4 and len(tokens) > 2:
        third = tokens[2]
        for key in (
            (top_code, third),
            (top_code, _normalize_leaf_name(third)),
        ):
            leaf_parent = _LEAF_TO_PARENT.get(key)
            leaf_obj = _LEAF_BY_NAME.get(key)
            if leaf_parent and leaf_parent["code"] == sub["code"] and leaf_obj:
                # 경기도: 1depth=시군구, 2depth=읍면동
                if top_code == "09":
                    return sub["code"], sub["name"], leaf_obj["code"], leaf_obj["name"]
                return top_code, top_name, leaf_obj["code"], leaf_obj["name"]

    # 경기도이고 sub가 leaf(6자리)로 바로 매칭된 경우 → 부모 시군구를 1depth로
    if top_code == "09" and len(sub["code"]) == 6:
        parent = _LEAF_TO_PARENT.get((top_code, sub["name"]))
        if parent:
            return parent["code"], parent["name"], sub["code"], sub["name"]

    return top_code, top_name, sub["code"], sub["name"]


def _find_sub(top_code: str, token: str) -> Optional[dict]:
    """top_code 안에서 token 으로 sub 검색.
    - 시/군/구 직접 매칭 → sub 객체 반환
    - 읍/면/동 매칭 → leaf 객체 반환 (code가 6자리)
    """
    # 정확 일치
    hit = _SUB_BY_TOP_NAME.get((top_code, token))
    if hit:
        return hit
    # 정규화 일치 (강남 → 강남구)
    hit = _SUB_BY_TOP_NAME.get((top_code, _normalize_sub_name(token)))
    if hit:
        return hit
    # leaf 일치 (운정 → 운정동) → leaf 객체 직접 반환
    hit = _LEAF_BY_NAME.get((top_code, token))
    if hit:
        return hit
    hit = _LEAF_BY_NAME.get((top_code, _normalize_leaf_name(token)))
    if hit:
        return hit
    return None


def _try_match_as_sub(
    first_token: str,
    second_token: str,
    exclude_top: str,
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """
    첫 토큰이 시/도 이름이기도 하지만 다른 시/도의 sub이기도 한 경우 처리.

    ex) "광주 오포 청소"
      - first_token="광주", second_token="오포", exclude_top="05"(광주광역시)
      - 경기도(09) 안에서 "광주시"(0905) sub 발견
      - 광주시(0905)의 leaf 중 "오포동"(090512) 발견 → 부모가 광주시 ✅
      - 반환: ("09", "경기", "0905", "광주시")

    매칭 성공하는 시/도가 여러 개면 첫 번째만 반환.
    """
    for top_code, top in REGIONS.items():
        if top_code == exclude_top:
            continue

        # ① 첫 토큰이 이 시/도의 sub인가?
        parent_sub = _find_sub(top_code, first_token)
        if parent_sub is None:
            continue

        # ② 두 번째 토큰이 parent_sub 의 leaf(읍/면/동)에 있는가?
        for key in (
            (top_code, second_token),
            (top_code, _normalize_leaf_name(second_token)),
        ):
            leaf_parent = _LEAF_TO_PARENT.get(key)
            leaf_obj = _LEAF_BY_NAME.get(key)
            if leaf_parent and leaf_parent["code"] == parent_sub["code"] and leaf_obj:
                # 경기도: 1depth=시군구, 2depth=읍면동
                if top_code == "09":
                    return parent_sub["code"], parent_sub["name"], leaf_obj["code"], leaf_obj["name"]
                return top_code, top["name"], leaf_obj["code"], leaf_obj["name"]

        # ③ leaf 데이터가 없는 시/도(강원, 경남 등)는 첫 토큰 sub 매칭만으로 반환.
        #    단, 이 시/도에 leaf 자체가 아예 없을 때만 허용(leaf가 있는데 못 찾은 건 제외).
        if not parent_sub.get("subs"):
            return top_code, top["name"], parent_sub["code"], parent_sub["name"]

    return None, None, None, None


def _try_match_without_top(
    tokens: list[str],
) -> tuple[Optional[str], Optional[str], Optional[str], Optional[str]]:
    """첫 토큰이 시/도가 아닐 때, 모든 시/도에서 첫 토큰을 sub로 검색.
    두 번째 토큰이 있으면 그것까지 leaf 매칭 시도.

    ex) "파주 교하 소독업체"
      - "파주" → 경기 파주시(0927) 매칭
      - "교하" → 파주시 leaf 중 교하동(092711) 매칭
      - 반환: ("09", "경기", "092711", "교하동")
    """
    first = tokens[0]
    second = tokens[1] if len(tokens) > 1 else None

    for top_code, top in REGIONS.items():
        sub = _find_sub(top_code, first)
        if sub is None:
            continue

        # 두 번째 토큰으로 leaf 매칭 시도
        if second:
            for key in (
                (top_code, second),
                (top_code, _normalize_leaf_name(second)),
            ):
                leaf_parent = _LEAF_TO_PARENT.get(key)
                leaf_obj = _LEAF_BY_NAME.get(key)
                if leaf_parent and leaf_parent["code"] == sub["code"] and leaf_obj:
                    # 경기도: 1depth=시군구, 2depth=읍면동
                    if top_code == "09":
                        return sub["code"], sub["name"], leaf_obj["code"], leaf_obj["name"]
                    return top_code, top["name"], leaf_obj["code"], leaf_obj["name"]

        # leaf 매칭 실패 or 두 번째 토큰 없음 → 시군구까지만 반환
        return top_code, top["name"], sub["code"], sub["name"]

    return None, None, None, None