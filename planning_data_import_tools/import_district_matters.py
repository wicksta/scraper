#!/usr/bin/env python3
"""Manual XLSX -> existing District Matters CSV; Python standard library only."""
import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import posixpath
import re
import shutil
import tempfile
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from urllib.request import urlopen
from urllib.parse import urlparse
import zipfile
from xml.etree import ElementTree as ET

HERE = Path(__file__).resolve().parent
DEFAULT_SOURCE = 'https://assets.publishing.service.gov.uk/media/6ab28022997a4b2950cced21/Planning_Performance_Dashboard_Table.xlsx'
DEFAULT_OUTPUT = Path('/mnt/ngist/public_html/dashboard/data/2025_District_Matters_Cleaned.csv')
NS = {'m': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
REL = '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id'


def read_source(source):
    if source.startswith('https://'):
        parsed = urlparse(source)
        if parsed.hostname != 'assets.publishing.service.gov.uk' or not parsed.path.endswith('.xlsx'):
            raise ValueError('Source must be an official GOV.UK XLSX attachment')
        with urlopen(source, timeout=30) as response:
            data = response.read(20 * 1024 * 1024 + 1)
    else:
        data = Path(source).read_bytes()
    if len(data) > 20 * 1024 * 1024:
        raise ValueError('Workbook exceeds 20 MiB')
    return data


def worksheet(data):
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        shared = []
        if 'xl/sharedStrings.xml' in archive.namelist():
            shared = [''.join(node.itertext()) for node in ET.fromstring(archive.read('xl/sharedStrings.xml')).findall('m:si', NS)]
        workbook = ET.fromstring(archive.read('xl/workbook.xml'))
        sheets = [s for s in workbook.findall('m:sheets/m:sheet', NS) if s.get('name') == 'District Matters Summary Table']
        if len(sheets) != 1:
            raise ValueError('Expected one District Matters Summary Table')
        relationships = ET.fromstring(archive.read('xl/_rels/workbook.xml.rels'))
        targets = {r.get('Id'): r.get('Target') for r in relationships}
        target = targets[sheets[0].get(REL)]
        member = target.lstrip('/') if target.startswith('/') else posixpath.normpath('xl/' + target)
        sheet = ET.fromstring(archive.read(member))
        rows = {}
        for row in sheet.findall('m:sheetData/m:row', NS):
            values = {}
            for cell in row.findall('m:c', NS):
                column = re.sub(r'\d', '', cell.get('r', ''))
                value = cell.find('m:v', NS)
                raw = value.text if value is not None else ''
                if cell.get('t') == 's' and raw:
                    raw = shared[int(raw)]
                elif cell.get('t') == 'inlineStr':
                    raw = ''.join(cell.find('m:is', NS).itertext())
                elif cell.find('m:f', NS) is not None and value is None:
                    raise ValueError('Formula has no saved result: ' + cell.get('r'))
                values[column] = raw or ''
            rows[int(row.get('r'))] = values
        return rows


def numeric(value, percentage=False):
    raw = value.strip().replace(',', '')
    if raw in ('', '-', '..', 'z', 'c', 'x'):
        return None
    try:
        number = Decimal(raw)
    except InvalidOperation as error:
        raise ValueError('Unexpected numeric value: ' + raw) from error
    if not number.is_finite() or number < 0 or (percentage and number > 100):
        raise ValueError('Invalid count or percentage: ' + raw)
    if not percentage and number != number.to_integral_value():
        raise ValueError('Non-integer count: ' + raw)
    return number


def formatted(number):
    if number is None:
        return ''
    return format(number.quantize(Decimal('0.000001')), 'f').rstrip('0').rstrip('.')


def weighted(row, count_columns, rate_columns):
    counts = [numeric(row.get(c, '')) for c in count_columns]
    if any(c is None for c in counts):
        return None
    total = sum(counts)
    if total == 0:
        return None
    result = Decimal(0)
    for count, column in zip(counts, rate_columns):
        if count == 0:
            continue
        rate = numeric(row.get(column, ''), True)
        if rate is None:
            return None
        result += count * rate
    return result / total


def build_csv(data):
    rows = worksheet(data)
    title = rows.get(1, {}).get('A', '')
    period = re.search(r'year ending ([A-Za-z]+ \d{4})\b', title, re.I)
    if not period:
        raise ValueError('Cannot identify reporting period')
    # Validate semantic headers before using the compatibility column layout.
    checks = {('A', 4): 'Planning authority', ('B', 4): 'ONS code',
        ('W', 6): 'Number of decisions on applications for major development',
        ('AE', 6): 'Number of decisions on applications for minor residential development',
        ('AI', 6): 'Number of decisions on applications for non-major development (excluding householder development and residential)',
        ('AM', 6): 'Number of decisions on non-major development'}
    for (column, number), expected in checks.items():
        if rows.get(number, {}).get(column, '').strip() != expected:
            raise ValueError(f'Workbook layout changed at {column}{number}; refusing to guess')
    headers = json.loads((HERE / 'district_csv_headers.json').read_text())
    output = []
    seen = set()
    for number, row in sorted(rows.items()):
        code = row.get('B', '').strip()
        if not re.fullmatch(r'E\d{8}', code):
            continue
        if code in seen:
            raise ValueError('Duplicate ONS code: ' + code)
        seen.add(code)
        # Old 26-column CSV layout: overview, majors, householders,
        # non-majors excluding householders, then combined non-major summary.
        values = [row.get('A', ''), code]
        columns = ['C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'W', 'X', 'Y', 'Z', 'AA', 'AB', 'AC', 'AD']
        count_columns = {'C', 'D', 'G', 'I', 'J', 'W', 'AA', 'AM'}
        values += [formatted(numeric(row.get(c, ''), c not in count_columns)) for c in columns]
        counts = [numeric(row.get(c, '')) for c in ['AE', 'AI']]
        values.append(formatted(sum(counts)) if all(c is not None for c in counts) else '')
        for rate_columns in [('AF', 'AJ'), ('AG', 'AK'), ('AH', 'AL')]:
            values.append(formatted(weighted(row, ['AE', 'AI'], rate_columns)))
        values += [formatted(numeric(row.get(c, ''), c != 'AM')) for c in ['AM', 'AN', 'AO', 'AP']]
        if len(values) != len(headers):
            raise ValueError('CSV column count mismatch')
        output.append(values)
    if len(output) < 250 or 'E09000033' not in seen or 'E92000001' not in seen:
        raise ValueError('Incomplete authority coverage')
    buffer = io.StringIO(newline='')
    writer = csv.writer(buffer)
    writer.writerow(headers)
    writer.writerows(output)
    return buffer.getvalue().encode('utf-8-sig'), {'period': period.group(1), 'title': title,
        'authority_count': len(output), 'non_major_definition': 'Excludes householders; residential and other decisions combined with decision-weighted percentages.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', default=DEFAULT_SOURCE, help='Official XLSX URL or local file')
    parser.add_argument('--output', type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument('--dry-run', action='store_true', help='Validate and report; do not write')
    args = parser.parse_args()
    source = read_source(args.source)
    csv_bytes, metadata = build_csv(source)
    old = args.output.read_bytes() if args.output.exists() else None
    print(f"District Matters: year ending {metadata['period']}; {metadata['authority_count']} rows; destination {args.output}")
    print('CSV ' + ('unchanged' if old == csv_bytes else 'would change' if args.dry_run else 'updated'))
    if args.dry_run:
        return
    if not args.output.parent.is_dir():
        raise ValueError('Output directory does not exist')
    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
    metadata.update(source=args.source, source_sha256=hashlib.sha256(source).hexdigest(),
        csv_sha256=hashlib.sha256(csv_bytes).hexdigest(), generated_at=datetime.now(timezone.utc).isoformat())
    metadata_path = args.output.with_suffix('.metadata.json')
    if old is not None and old != csv_bytes:
        backup = HERE / '.state' / 'district-matters' / hashlib.sha256(str(args.output.resolve()).encode()).hexdigest()[:16] / stamp
        backup.mkdir(parents=True)
        shutil.copy2(args.output, backup / args.output.name)
        if metadata_path.exists():
            shutil.copy2(metadata_path, backup / metadata_path.name)
        print('Backup: ' + str(backup))
    for target, content in [(args.output, csv_bytes), (metadata_path, (json.dumps(metadata, indent=2) + '\n').encode())]:
        if target == args.output and old == csv_bytes:
            continue
        descriptor, temporary = tempfile.mkstemp(prefix='.district-', dir=target.parent)
        try:
            with os.fdopen(descriptor, 'wb') as handle:
                handle.write(content)
            os.chmod(temporary, target.stat().st_mode & 0o777 if target.exists() else 0o644)
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)


if __name__ == '__main__':
    try:
        main()
    except Exception as error:
        raise SystemExit(str(error))
