#!/usr/bin/env python3
import argparse
import pathlib
import pandas as pd
from datetime import date, timedelta
import sys

def generate_data(prefix: str, rows: int) -> pd.DataFrame:
    data = []
    categories = ['A', 'B', 'C']
    today = date.today()
    for i in range(1, rows + 1):
        data.append({
            'id': i,
            'name': f'{prefix}_item_{i}',
            'value': round(i * 1.1, 2),
            'date': today - timedelta(days=i),
            'flag': i % 2 == 0,
            'category': categories[(i - 1) % len(categories)]
        })
    return pd.DataFrame(data)

def write_files(df: pd.DataFrame, path_prefix: pathlib.Path) -> None:
    path_prefix.parent.mkdir(parents=True, exist_ok=True)
    csv_path = path_prefix.with_suffix('.csv')
    json_path = path_prefix.with_suffix('.json')
    parquet_path = path_prefix.with_suffix('.parquet')
    xlsx_path = path_prefix.with_suffix('.xlsx')
    try:
        df.to_csv(csv_path, index=False)
        print(f'Written CSV: {csv_path}')
    except Exception as e:
        print(f'Error writing CSV to {csv_path}: {e}', file=sys.stderr)
    try:
        df.to_json(json_path, orient='records', date_format='iso')
        print(f'Written JSON: {json_path}')
    except Exception as e:
        print(f'Error writing JSON to {json_path}: {e}', file=sys.stderr)
    try:
        df.to_parquet(parquet_path, index=False)
        print(f'Written Parquet: {parquet_path}')
    except Exception as e:
        print(f'Error writing Parquet to {parquet_path}: {e}', file=sys.stderr)
    try:
        df.to_excel(xlsx_path, index=False)
        print(f'Written Excel: {xlsx_path}')
    except Exception as e:
        print(f'Error writing Excel to {xlsx_path}: {e}', file=sys.stderr)

def main():
    parser = argparse.ArgumentParser(description="Generate sample fixture files in CSV, JSON, Parquet, and Excel formats.")
    parser.add_argument(
        '--base-dir',
        type=pathlib.Path,
        default=pathlib.Path('/Users/vitruves/Developer/GitHub/nail/tests/fixtures'),
        help="Base directory for fixture files (default: /Users/vitruves/Developer/GitHub/nail/tests/fixtures)"
    )
    parser.add_argument(
        '--sample-rows',
        type=int,
        default=5,
        help="Number of rows for 'sample' dataset (default: 5)"
    )
    parser.add_argument(
        '--sample2-rows',
        type=int,
        default=8,
        help="Number of rows for 'sample2' dataset (default: 8)"
    )
    args = parser.parse_args()

    fixtures_dir = args.base_dir
    for prefix, rows in [('sample', args.sample_rows), ('sample2', args.sample2_rows)]:
        path_prefix = fixtures_dir / prefix
        df = generate_data(prefix, rows)
        write_files(df, path_prefix)

if __name__ == '__main__':
    main()