"""
firestore_store.py

Firestore 연결, 스키마 변환, 진행 상태 관리.

스키마:
  places/{place_id}              ← 업체 정보 (place_id 로 dedup)
  crawl_jobs/{keyword_id}        ← 키워드별 처리 상태
  crawl_state/global             ← 전역 진행 인덱스
"""
from __future__ import annotations

import math
import re
import hashlib
from typing import Optional

import firebase_admin
from firebase_admin import firestore
from google.cloud.firestore_v1 import ArrayUnion, SERVER_TIMESTAMP


# ---------------------------------------------------------------------------
# Firebase 초기화
# ---------------------------------------------------------------------------

_app = None


def init_firebase() -> None:
    """GOOGLE_APPLICATION_CREDENTIALS 환경변수로 자동 인증."""
    global _app
    if _app is not None:
        return
    if not firebase_admin._apps:
        _app = firebase_admin.initialize_app()
    else:
        _app = firebase_admin.get_app()


def get_db():
    init_firebase()
    return firestore.client()


# ---------------------------------------------------------------------------
# 유틸
# ---------------------------------------------------------------------------

PLACE_ID_RE = re.compile(r"/place/(\d+)")


def extract_place_id(detail_url: str) -> Optional[str]:
    """상세페이지URL에서 place_id 추출."""
    if not detail_url or not isinstance(detail_url, str):
        return None
    m = PLACE_ID_RE.search(detail_url)
    return m.group(1) if m else None


def normalize_keyword(keyword: str) -> str:
    """Firestore document ID 로 안전하게 변환."""
    return keyword.strip().replace(" ", "_").replace("/", "_")


def clean_value(v):
    """NaN, inf → None. 빈 문자열 → None."""
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
        return v
    if isinstance(v, str):
        v = v.strip()
        return v if v else None
    return v


# ---------------------------------------------------------------------------
# PlaceRow → Firestore document
# ---------------------------------------------------------------------------

FIELD_MAP = {
    "이름": "name",
    "카테고리": "category",
    "도로명주소": "address_road",
    "지번주소": "address_jibun",
    "일반전화": "phone",
    "안심번호": "virtual_phone",
    "전화번호": "phone_main",
    "영업시간": "business_hours",
    "썸네일이미지URL": "thumbnail_url",
    "방문자 리뷰수": "visitor_review_count",
    "방문자 평점": "visitor_rating",
    "블로그 리뷰수": "blog_review_count",
    "위도": "lat",
    "경도": "lng",
    "매장정보": "store_info",
    "부가설명": "description",
    "홈페이지URL": "homepage_url",
    "사진리뷰수": "photo_review_count",
    "상세페이지URL": "detail_url",
    "해시태그": "hashtag",
}


def placerow_to_doc(row_dict: dict) -> Optional[tuple[str, dict]]:
    """PlaceRow.to_dict() 결과 → (place_id, firestore doc dict).
    place_id 추출 실패 시 이름+주소 해시로 폴백.
    """
    detail_url = clean_value(row_dict.get("상세페이지URL"))
    place_id = extract_place_id(detail_url) if detail_url else None

    if not place_id:
        name = clean_value(row_dict.get("이름")) or ""
        addr = (
            clean_value(row_dict.get("도로명주소"))
            or clean_value(row_dict.get("지번주소"))
            or ""
        )
        if not name:
            return None
        place_id = "hash_" + hashlib.md5(f"{name}|{addr}".encode()).hexdigest()[:16]

    doc = {}
    for kor, eng in FIELD_MAP.items():
        if kor in row_dict:
            doc[eng] = clean_value(row_dict[kor])

    return place_id, doc


# ---------------------------------------------------------------------------
# Firestore 쓰기
# ---------------------------------------------------------------------------

def upload_rows_to_firestore(rows: list, keyword: str) -> dict:
    """PlaceRow 리스트 → places 컬렉션 batch upsert.

    한 업체가 여러 키워드에 잡히는 경우 keywords 배열에 누적 (ArrayUnion).
    """
    db = get_db()
    uploaded = 0
    skipped = 0
    place_ids: list[str] = []

    batch = db.batch()
    batch_count = 0

    for row in rows:
        row_dict = row.to_dict() if hasattr(row, "to_dict") else row
        result = placerow_to_doc(row_dict)
        if not result:
            skipped += 1
            continue

        place_id, doc = result
        place_ids.append(place_id)

        doc["keywords"] = ArrayUnion([keyword])
        doc["updated_at"] = SERVER_TIMESTAMP

        ref = db.collection("places").document(place_id)
        batch.set(ref, doc, merge=True)
        batch_count += 1
        uploaded += 1

        if batch_count >= 400:  # Firestore 배치 한도 500, 안전 마진
            batch.commit()
            batch = db.batch()
            batch_count = 0

    if batch_count > 0:
        batch.commit()

    return {"uploaded": uploaded, "skipped": skipped, "place_ids": place_ids}


def mark_job_done(keyword: str, place_count: int, place_ids: list[str]) -> None:
    db = get_db()
    job_id = normalize_keyword(keyword)
    db.collection("crawl_jobs").document(job_id).set({
        "keyword": keyword,
        "status": "done",
        "place_count": place_count,
        "place_ids_sample": place_ids[:50],
        "last_attempt_at": SERVER_TIMESTAMP,
    }, merge=True)


def mark_job_failed(keyword: str, error: str, retries: int) -> None:
    db = get_db()
    job_id = normalize_keyword(keyword)
    db.collection("crawl_jobs").document(job_id).set({
        "keyword": keyword,
        "status": "failed",
        "error": str(error)[:1000],
        "retries": retries,
        "last_attempt_at": SERVER_TIMESTAMP,
    }, merge=True)


def is_keyword_done(keyword: str) -> bool:
    db = get_db()
    job_id = normalize_keyword(keyword)
    doc = db.collection("crawl_jobs").document(job_id).get()
    if not doc.exists:
        return False
    return doc.to_dict().get("status") == "done"


# ---------------------------------------------------------------------------
# 전역 인덱스 (트랜잭션 — 동시 job 안전)
# ---------------------------------------------------------------------------

STATE_DOC_PATH = "crawl_state/global"


def claim_next_index(total: int) -> Optional[int]:
    """다음 처리할 인덱스를 원자적으로 클레임. total 도달 시 None."""
    db = get_db()
    state_ref = db.document(STATE_DOC_PATH)
    transaction = db.transaction()

    @firestore.transactional
    def _claim(tx):
        snap = state_ref.get(transaction=tx)
        if snap.exists:
            current = snap.to_dict().get("last_claimed_index", -1)
        else:
            current = -1

        next_idx = current + 1
        if next_idx >= total:
            return None

        tx.set(state_ref, {
            "last_claimed_index": next_idx,
            "total": total,
            "updated_at": SERVER_TIMESTAMP,
        }, merge=True)
        return next_idx

    return _claim(transaction)


def get_progress() -> dict:
    db = get_db()
    snap = db.document(STATE_DOC_PATH).get()
    if not snap.exists:
        return {"last_claimed_index": -1}
    return snap.to_dict()


def reset_progress() -> None:
    """전역 인덱스 초기화. is_keyword_done 가 중복 방지하므로 데이터 안전."""
    db = get_db()
    db.document(STATE_DOC_PATH).set({
        "last_claimed_index": -1,
        "updated_at": SERVER_TIMESTAMP,
    })
