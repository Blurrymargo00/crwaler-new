"""
command_parser.py

crawler_commands.txt 의 명령 라인 파싱.
run_all.py 와 dry_run_mapping.py 가 공유하는 단순 유틸.
무거운 의존성 없음.
"""
from __future__ import annotations

import re

# crawler_commands.txt 명령 형식:
#   python naver_map_crawler.py "서울 강남 입주청소" --max 20
CMD_RE = re.compile(r'"\s*([^"]+?)\s*"\s*(?:--max\s+(\d+))?')


def parse_command(cmd: str) -> tuple[str, int]:
    """명령 라인에서 (keyword, max_items) 추출.

    max 옵션 없으면 50 (크롤러 기본값).
    매칭 실패 시 ValueError.
    """
    m = CMD_RE.search(cmd)
    if not m:
        raise ValueError(f"명령 파싱 실패: {cmd}")
    keyword = m.group(1).strip()
    max_items = int(m.group(2)) if m.group(2) else 50
    return keyword, max_items
