"""
retry_failed.py

crawl_jobs 에서 status=='failed' 인 키워드만 골라 재실행.
전역 인덱스와 무관하게 동작.

환경변수:
  SLEEP_SEC          기본 120 (실패한 애들이라 더 길게)
  MAX_RETRIES        기본 3
  RETRY_BASE_SLEEP   기본 240
  MAX_RUNTIME_SEC    기본 20400
  DEFAULT_MAX        기본 20 (max_items)
"""
from __future__ import annotations

import os
import sys
import time

from firestore_store import (
    init_firebase,
    get_db,
    upload_rows_to_firestore,
    mark_job_done,
    mark_job_failed,
)
from run_all import run_one_keyword, log


SLEEP_SEC = int(os.environ.get("SLEEP_SEC", "120"))
MAX_RUNTIME_SEC = int(os.environ.get("MAX_RUNTIME_SEC", "20400"))
DEFAULT_MAX = int(os.environ.get("DEFAULT_MAX", "20"))


def main() -> int:
    init_firebase()
    db = get_db()

    failed_jobs = []
    for doc in db.collection("crawl_jobs").where("status", "==", "failed").stream():
        failed_jobs.append(doc.to_dict())

    log(f"실패 키워드 {len(failed_jobs)}개 발견")
    if not failed_jobs:
        return 0

    started = time.time()
    ok = 0
    still_fail = 0

    for job in failed_jobs:
        if time.time() - started > MAX_RUNTIME_SEC:
            log("⏱  시간 초과 — 종료")
            break

        keyword = job.get("keyword")
        if not keyword:
            continue

        prev_retries = int(job.get("retries", 0))
        log(f"▶  retry: {keyword} (prev_retries={prev_retries})")

        success, rows, err = run_one_keyword(keyword, DEFAULT_MAX)
        if success:
            try:
                result = upload_rows_to_firestore(rows, keyword)
                mark_job_done(keyword, result["uploaded"], result["place_ids"])
                log(f"  ✅ {result['uploaded']}건")
                ok += 1
            except Exception as e:
                log(f"  ❌ upload: {e}")
                mark_job_failed(keyword, f"firestore upload: {e}", prev_retries + 1)
                still_fail += 1
        else:
            log(f"  ❌ {err[:200]}")
            mark_job_failed(keyword, err, prev_retries + 1)
            still_fail += 1

        time.sleep(SLEEP_SEC)

    log(f"완료 — 성공:{ok}, 여전히 실패:{still_fail}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
