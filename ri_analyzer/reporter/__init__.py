"""ri_analyzer.reporter パッケージ

後方互換性のため、全パブリック関数をここで再エクスポートする。
外部からは `from ri_analyzer import reporter; reporter.print_coverage(...)` で使用できる。
"""

from ri_analyzer.reporter._base import set_color  # noqa: F401
from ri_analyzer.reporter.ce_sections import (     # noqa: F401
    print_expiration,
    print_coverage,
    print_utilization,
    print_recommendations,
)
from ri_analyzer.reporter.cur_sections import (    # noqa: F401
    print_cur_instances,
    print_cur_coverage,
    print_unused_ri,
    print_ce_factcheck,
)
