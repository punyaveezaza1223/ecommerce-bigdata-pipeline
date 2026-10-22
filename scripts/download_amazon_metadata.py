#!/usr/bin/env python3
"""Download only official Amazon Reviews 2023 product metadata (.jsonl.gz).
No reviews, images or decompressed copies. Resumable via HTTP Range when supported.
"""
import argparse
import shutil
import urllib.error
import urllib.request
from pathlib import Path

BASE_URL = 'https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_2023/raw/meta_categories/'
ALLOWED = {'Amazon_Fashion', 'All_Beauty', 'Appliances', 'Arts_Crafts_and_Sewing', 'Beauty_and_Personal_Care'}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--category', choices=sorted(ALLOWED), default='Amazon_Fashion')
    ap.add_argument('--out-dir', default='/workspace/data/raw/historical/amazon_2023')
    ap.add_argument('--max-mb', type=int, default=1600, help='Max compressed file size in MiB; abort if exceeded')
    args = ap.parse_args()
    folder = Path(args.out_dir)
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / f'meta_{args.category}.jsonl.gz'
    partial = target.with_suffix(target.suffix + '.part')
    if target.exists():
        print(f'ALREADY DOWNLOADED: {target} ({target.stat().st_size:,} bytes)')
        return
    url = BASE_URL + target.name
    maximum = args.max_mb * 1024 * 1024
    existing = partial.stat().st_size if partial.exists() else 0
    request = urllib.request.Request(url, headers={
        'User-Agent': 'EcommerceResearchPipeline/1.0',
        **({'Range': f'bytes={existing}-'} if existing else {}),
    })
    print('SOURCE:', url)
    print('DESTINATION:', target)
    print('MAX COMPRESSED MiB:', args.max_mb)
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            status = response.status
            if existing and status != 206:
                # Server did not honor resume; restart safely rather than append corrupt bytes.
                existing = 0
                partial.unlink(missing_ok=True)
            length = response.headers.get('Content-Length')
            if length and existing + int(length) > maximum:
                raise RuntimeError('File exceeds --max-mb; stopped before downloading')
            mode = 'ab' if existing else 'wb'
            written = existing
            with partial.open(mode) as output:
                while True:
                    chunk = response.read(1024 * 1024)
                    if not chunk:
                        break
                    written += len(chunk)
                    if written > maximum:
                        raise RuntimeError('Download reached disk safety limit; partial file retained')
                    output.write(chunk)
                    if written % (64 * 1024 * 1024) < len(chunk):
                        free = shutil.disk_usage(folder).free
                        print(f'DOWNLOADED MiB: {written / 1048576:.0f}; FREE GiB: {free / 1073741824:.1f}', flush=True)
                        if free < 12 * 1024**3:
                            raise RuntimeError('Free disk below 12 GiB; partial file retained')
    except (urllib.error.URLError, OSError) as exc:
        raise SystemExit(f'Download interrupted; rerun to resume if supported: {exc}')
    import gzip
    with partial.open('rb') as stream:
        if stream.read(2) != b'\x1f\x8b':
            raise RuntimeError('Downloaded file is not gzip; inspect source')
    # Test CRC by streaming file (no decompressed disk copy).
    with gzip.open(partial, 'rb') as stream:
        while stream.read(4 * 1024 * 1024):
            pass
    partial.rename(target)
    print(f'SUCCESS: {target} ({target.stat().st_size:,} compressed bytes)')

if __name__ == '__main__':
    main()
