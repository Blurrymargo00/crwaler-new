"""
check_progress.py

로컬에서 Firestore 진행 상태 확인.

실행:
  GOOGLE_APPLICATION_CREDENTIALS=path/to/firebase.json python check_progress.py
"""
from firestore_store import init_firebase, get_progress, get_db


def main() -> int:
    init_firebase()
    db = get_db()

    progress = get_progress()
    last = progress.get("last_claimed_index", -1)
    total = progress.get("total", "?")

    if isinstance(total, int) and total > 0:
        pct = (last + 1) / total * 100
        print(f"전역 인덱스: {last + 1} / {total}  ({pct:.1f}%)")
    else:
        print(f"전역 인덱스: {last + 1} / {total}")

    done = 0
    failed = 0
    failed_keywords = []
    for doc in db.collection("crawl_jobs").stream():
        d = doc.to_dict()
        if d.get("status") == "done":
            done += 1
        elif d.get("status") == "failed":
            failed += 1
            failed_keywords.append(d.get("keyword"))

    print(f"\ncrawl_jobs:")
    print(f"  ✅ done:   {done}")
    print(f"  ❌ failed: {failed}")

    if failed_keywords:
        print(f"\n실패 키워드 (상위 20):")
        for kw in failed_keywords[:20]:
            print(f"  - {kw}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
