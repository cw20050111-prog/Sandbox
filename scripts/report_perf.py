#!/usr/bin/env python3
"""将单次接口 JSONL 导出为 CSV；分位数按 nearest-rank 计算。"""
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path


def percentile(sorted_values: list[int], probability: float) -> int | None:
    if not sorted_values:
        return None
    return sorted_values[max(0, math.ceil(probability * len(sorted_values)) - 1)]


def summarize(records: list[dict]) -> list[dict]:
    groups = defaultdict(list)
    # 分开统计真实 SDK 桥接与 clock_only，避免把离线数据混到部署数据中。
    for record in records:
        groups[(record['method'], record['perf_backend'])].append(record)
    rows = []
    for (method, backend), items in sorted(groups.items()):
        for population in ('all_completed', 'success_only'):
            selected = [r for r in items if isinstance(r.get('total_ns'), int)
                        and r.get('rc') is not None and r.get('outcome') != 'abandon'
                        and (population == 'all_completed' or r.get('rc') == 0)]
            values = sorted(r['total_ns'] for r in selected)
            def ms(value):
                return None if value is None else value / 1_000_000
            rows.append(dict(method=method, perf_backend=backend, population=population,
                call_records=len(items), completed_samples=len(values),
                good=sum(r.get('rc') == 0 for r in items),
                bad=sum(r.get('rc') is not None and r.get('rc') != 0 for r in items),
                abandoned=sum(r.get('outcome') == 'abandon' for r in items),
                avg_ms=ms(sum(values) / len(values)) if values else None,
                min_ms=ms(values[0]) if values else None,
                p99_ms=ms(percentile(values, .99)), p999_ms=ms(percentile(values, .999)),
                max_ms=ms(values[-1]) if values else None))
    return rows


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open('w', encoding='utf-8-sig', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('jsonl', type=Path)
    parser.add_argument('--out-dir', type=Path, default=Path('out/report'))
    args = parser.parse_args()
    records = []
    with args.jsonl.open(encoding='utf-8') as file:
        for line_number, line in enumerate(file, 1):
            if not line.strip():
                continue
            record = json.loads(line)
            if (not isinstance(record, dict) or not isinstance(record.get('method'), str)
                    or not isinstance(record.get('perf_backend'), str)):
                raise ValueError(f'第 {line_number} 行不是接口计时记录')
            records.append(record)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    summary = summarize(records)
    fields = list(dict.fromkeys(key for row in records for key in row))
    if not fields:
        fields = ['method', 'perf_backend', 'request_id', 'total_ns', 'rc', 'outcome']
    write_csv(args.out_dir / 'interface_calls.csv', records, fields)
    write_csv(args.out_dir / 'interface_summary.csv', summary,
        ['method', 'perf_backend', 'population', 'call_records', 'completed_samples',
         'good', 'bad', 'abandoned', 'avg_ms', 'min_ms', 'p99_ms', 'p999_ms', 'max_ms'])
    print(json.dumps({'records': len(records), 'groups': len(summary),
        'calls_csv': str(args.out_dir / 'interface_calls.csv'),
        'summary_csv': str(args.out_dir / 'interface_summary.csv'),
        'percentile_method': 'nearest_rank',
        'note': '接口分位数来自逐次 JSONL；少量样本时 P99/P999 通常等于 max。'},
        ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
