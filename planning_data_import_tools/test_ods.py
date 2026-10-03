"""Small, generated workbooks exercise ODS format handling and rejection paths."""
import io
import unittest
import zipfile
from xml.sax.saxutils import escape
from read_ods import discover, read_ods, OFFICE, TABLE, TEXT


def cell(value, repeat=1, numeric=None):
    attr = f' table:number-columns-repeated="{repeat}"'
    if numeric is not None:
        attr += f' office:value-type="float" office:value="{numeric}"'
    return f'<table:table-cell{attr}><text:p>{escape(str(value))}</text:p></table:table-cell>'


def workbook(duplicate=False, bad_total=False, symbol=False, mismatch=False):
    sheets = []
    for name, kind in [('P152a', 'major'), ('P154', 'non-major')]:
        headers = ['LADNM', 'LADCD', 'Quarter', f'Total {kind} application decisions',
                   f'{kind.capitalize()} applications not determined',
                   f'Total {kind} decisions and non-determined cases',
                   f'Total {kind} appeal decisions', f'{kind.capitalize()} decisions overturned at appeal']
        header = '<table:table-row>' + ''.join(cell(h) for h in headers) + '</table:table-row>'
        quarter = '2025 Q2' if mismatch and name == 'P154' else '2025 Q3'
        # Repeated zero cells represent appeals and overturns; the trailing 16k
        # repeated padding columns must not be expanded into huge arrays.
        data = ''.join(cell(v) for v in ['Test authority', 'E09000033', quarter])
        data += cell('[c]' if symbol else '1,022') + cell(3)
        data += cell('1,025', numeric=1024 if bad_total else 1025) + cell(0, repeat=2) + cell('', repeat=16376)
        row = f'<table:table-row>{data}</table:table-row>'
        sheets.append(f'<table:table table:name="{name}">{header}{row}{row if duplicate else ""}</table:table>')
    xml = f'<office:document-content xmlns:office="{OFFICE}" xmlns:table="{TABLE}" xmlns:text="{TEXT}"><office:body><office:spreadsheet>{"".join(sheets)}</office:spreadsheet></office:body></office:document-content>'
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('content.xml', xml)
    return output.getvalue()


class OdsTests(unittest.TestCase):
    def test_numeric_display_and_repeats(self):
        result = read_ods(workbook())
        self.assertEqual(result['quarters'], ['2025 Q3'])
        row = result['sheets']['P154'][0]
        self.assertEqual(row['application_decisions'], 1022)
        self.assertEqual(row['total_decisions_and_nondetermined'], 1025)
        self.assertEqual(row['appeal_decisions'], 0)

    def test_duplicate(self):
        with self.assertRaisesRegex(ValueError, 'Duplicate authority'):
            read_ods(workbook(duplicate=True))

    def test_bad_component_total(self):
        with self.assertRaisesRegex(ValueError, 'Component totals'):
            read_ods(workbook(bad_total=True))

    def test_unknown_symbol_is_not_zero(self):
        with self.assertRaisesRegex(ValueError, 'Invalid nonnegative integer'):
            read_ods(workbook(symbol=True))

    def test_mismatched_quarters(self):
        with self.assertRaisesRegex(ValueError, 'coverage differs'):
            read_ods(workbook(mismatch=True))

    def test_discovery_and_ambiguity(self):
        url = 'https://assets.publishing.service.gov.uk/media/test/new_file.ods'
        html = f'<a href="{url}">Planning Quality <span>Open Data</span></a>'
        self.assertEqual(discover(html)['url'], url)
        with self.assertRaisesRegex(ValueError, 'found 2'):
            discover(html + html.replace('new_file', 'other_file'))
        with self.assertRaisesRegex(ValueError, 'found 0'):
            discover(html.replace('assets.publishing.service.gov.uk', 'example.com'))


if __name__ == '__main__':
    unittest.main()
