#!/usr/bin/env python3
"""Read MHCLG ODS bytes (stdin) using only Python's standard library."""
import io
import json
import re
import sys
import zipfile
from decimal import Decimal, InvalidOperation
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree as ET

TABLE = "urn:oasis:names:tc:opendocument:xmlns:table:1.0"
TEXT = "urn:oasis:names:tc:opendocument:xmlns:text:1.0"
OFFICE = "urn:oasis:names:tc:opendocument:xmlns:office:1.0"
NS = {"t": TABLE, "x": TEXT}
PAGE = "https://www.gov.uk/government/statistical-data-sets/live-tables-on-planning-application-statistics"
FIELDS = ["application_decisions", "not_determined", "total_decisions_and_nondetermined",
          "appeal_decisions", "overturned_at_appeal"]


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self.href = None
        self.label = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            self.href = dict(attrs).get("href")
            self.label = []

    def handle_data(self, data):
        if self.href is not None:
            self.label.append(data)

    def handle_endtag(self, tag):
        if tag == "a" and self.href is not None:
            self.links.append((urljoin(PAGE, self.href), " ".join(self.label)))
            self.href = None


def discover(html):
    parser = Links()
    parser.feed(html)
    matches = set()
    for url, label in parser.links:
        label = re.sub(r"[^a-z0-9]+", " ", label.lower())
        parsed = urlparse(url)
        if ("planning quality" in label and "open data" in label
                and parsed.scheme == "https"
                and parsed.hostname == "assets.publishing.service.gov.uk"
                and parsed.path.lower().endswith(".ods")):
            matches.add(url)
    if len(matches) != 1:
        raise ValueError(f"Expected one Planning Quality ODS attachment, found {len(matches)}")
    return {"url": matches.pop()}


def text(cell):
    return " ".join("".join(p.itertext()) for p in cell.findall("x:p", NS)).strip()


def cells(row):
    result = []
    for cell in row:
        if cell.tag not in (f"{{{TABLE}}}table-cell", f"{{{TABLE}}}covered-table-cell"):
            continue
        repeat = int(cell.get(f"{{{TABLE}}}number-columns-repeated", "1"))
        result.extend([cell] * min(repeat, max(0, 64 - len(result))))
        if len(result) == 64:
            break
    return result


def count(cell, context):
    raw = cell.get(f"{{{OFFICE}}}value")
    if raw is None:
        displayed = text(cell)
        if not re.fullmatch(r"(?:\d+|\d{1,3}(?:,\d{3})+)", displayed):
            raise ValueError(f"Invalid nonnegative integer {displayed!r} at {context}")
        raw = displayed.replace(",", "")
    try:
        value = Decimal(raw)
        if not value.is_finite() or value < 0 or value != value.to_integral_value():
            raise ValueError()
        value = int(value)
        if value > 9007199254740991:
            raise ValueError()
        return value
    except (InvalidOperation, ValueError):
        raise ValueError(f"Invalid nonnegative integer {raw!r} at {context}") from None


def read_ods(data):
    with zipfile.ZipFile(io.BytesIO(data)) as archive:
        if archive.getinfo("content.xml").file_size > 64 * 1024 * 1024:
            raise ValueError("ODS content.xml exceeds 64 MiB")
        root = ET.fromstring(archive.read("content.xml"))
    output = {}
    for sheet in root.findall(".//t:table", NS):
        name = sheet.get(f"{{{TABLE}}}name")
        if name not in ("P152a", "P154"):
            continue
        if name in output:
            raise ValueError(f"Duplicate sheet {name}")
        kind = "major" if name == "P152a" else "non-major"
        prefix = kind.capitalize()
        required = ["LADNM", "LADCD", "Quarter", f"Total {kind} application decisions",
                    f"{prefix} applications not determined",
                    f"Total {kind} decisions and non-determined cases",
                    f"Total {kind} appeal decisions", f"{prefix} decisions overturned at appeal"]
        indices = None
        records = []
        keys = set()
        for row in sheet.findall("t:table-row", NS):
            values = cells(row)
            labels = [text(cell) for cell in values]
            if indices is None:
                if "LADCD" in labels and "Quarter" in labels:
                    if any(labels.count(header) != 1 for header in required):
                        raise ValueError(f"Missing or duplicate required columns in {name}")
                    indices = [labels.index(header) for header in required]
                continue
            if len(values) <= max(indices):
                if any(labels):
                    raise ValueError(f"Short row in {name}: {labels[:3]}")
                continue
            authority, code, quarter = (labels[index] for index in indices[:3])
            if not code and not quarter:
                continue  # Empty padding and publisher footnotes.
            if not re.fullmatch(r"E\d{8}", code) or not re.fullmatch(r"20\d{2} Q[1-4]", quarter):
                raise ValueError(f"Invalid authority/quarter in {name}: {code!r} / {quarter!r}")
            if not authority:
                raise ValueError(f"Missing authority name for {code}")
            key = (code, quarter)
            if key in keys or int(row.get(f"{{{TABLE}}}number-rows-repeated", "1")) != 1:
                raise ValueError(f"Duplicate authority/quarter in {name}: {key}")
            keys.add(key)
            numbers = {field: count(values[index], f"{name}/{code}/{quarter}/{field}")
                       for field, index in zip(FIELDS, indices[3:])}
            if numbers["application_decisions"] + numbers["not_determined"] != numbers["total_decisions_and_nondetermined"]:
                raise ValueError(f"Component totals do not match in {name}/{code}/{quarter}")
            if numbers["overturned_at_appeal"] > numbers["appeal_decisions"]:
                raise ValueError(f"Overturns exceed appeals in {name}/{code}/{quarter}")
            records.append({"code": code, "name": authority, "quarter": quarter, **numbers})
        if not records:
            raise ValueError(f"No data records in {name}")
        output[name] = records
    if set(output) != {"P152a", "P154"}:
        raise ValueError("Workbook must contain P152a and P154")
    quarters = [set(row["quarter"] for row in output[name]) for name in ("P152a", "P154")]
    if quarters[0] != quarters[1]:
        raise ValueError("P152a and P154 quarter coverage differs")
    ordered = sorted(quarters[0])
    expected = [f"{year} Q{q}" for year in range(int(ordered[0][:4]), int(ordered[-1][:4]) + 1)
                for q in range(1, 5) if ordered[0] <= f"{year} Q{q}" <= ordered[-1]]
    if ordered != expected:
        raise ValueError("Workbook has gaps in quarter coverage")
    return {"sheets": output, "quarters": ordered}


if __name__ == "__main__":
    try:
        data = sys.stdin.buffer.read(20 * 1024 * 1024 + 1)
        if len(data) > 20 * 1024 * 1024:
            raise ValueError("Input exceeds 20 MiB")
        result = discover(data.decode("utf-8")) if "--discover" in sys.argv else read_ods(data)
        print(json.dumps(result, separators=(",", ":")))
    except Exception as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
