#!/bin/bash
# Usage: ./ocr.sh input.pdf output.txt

inpdf="$1"
outtxt="$2"

# Convert PDF to images (PNG, 300 DPI recommended for OCR quality)
pdftoppm -r 300 "$inpdf" /tmp/ocr_page -png

# Run tesseract on all pages and append to output
rm -f "$outtxt"
for img in /tmp/ocr_page-*.png; do
    tesseract "$img" stdout >> "$outtxt"
done

# Clean up
rm /tmp/ocr_page-*.png