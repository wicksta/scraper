<?php
declare(strict_types=1);

function note_renderer_safe_text(mixed $value): string {
    if (!is_string($value)) {
        return '';
    }

    $text = str_replace(["\r\n", "\r"], "\n", $value);
    $text = preg_replace('/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/u', ' ', $text) ?? $text;
    $text = preg_replace('/\x{2028}|\x{2029}/u', "\n", $text) ?? $text;
    return trim($text);
}

function note_renderer_strip_heading_number_prefix(string $value): string {
    $text = trim($value);
    if ($text === '') {
        return '';
    }

    $text = preg_replace('/^\s*\d+(?:\.\d+)*\s*[\.\)\-:]?\s*/u', '', $text) ?? $text;
    return trim($text);
}

function note_renderer_strip_structural_tag_prefix(string $value): string {
    $text = trim($value);
    if ($text === '') {
        return '';
    }

    $text = preg_replace('/^\[(?:H1|H2|P|AP|Q)\]\s*/i', '', $text) ?? $text;
    return trim($text);
}

function note_renderer_extract_structural_tag_type(string $value): ?string {
    if (!preg_match('/^\[(H1|H2|P|AP|Q)\]\s*/i', trim($value), $m)) {
        return null;
    }

    return match (strtoupper((string)$m[1])) {
        'H1' => 'heading',
        'H2' => 'subheading',
        'P' => 'paragraph',
        'AP' => 'alpha_paragraph',
        'Q' => 'quote',
        default => null,
    };
}

function note_renderer_strip_alpha_prefix(string $value): string {
    $text = trim($value);
    if ($text === '') {
        return '';
    }

    $text = preg_replace('/^\s*[a-z]\s*[\.\)\-:]\s*/i', '', $text) ?? $text;
    return trim($text);
}

function note_renderer_normalize_result(array $decoded): array {
    $meta = is_array($decoded['meta'] ?? null) ? $decoded['meta'] : [];
    $contentBlocks = is_array($decoded['content_blocks'] ?? null) ? $decoded['content_blocks'] : [];

    if (count($contentBlocks) === 0) {
        $sections = is_array($decoded['sections'] ?? null) ? $decoded['sections'] : [];
        foreach ($sections as $section) {
            if (!is_array($section)) {
                continue;
            }

            $heading = note_renderer_safe_text((string)($section['heading'] ?? ''));
            $text = note_renderer_safe_text((string)($section['text'] ?? ''));
            if ($heading !== '') {
                $contentBlocks[] = ['type' => 'heading', 'text' => $heading];
            }
            if ($text !== '') {
                $contentBlocks[] = ['type' => 'paragraph', 'text' => $text];
            }

            $subsections = is_array($section['subsections'] ?? null) ? $section['subsections'] : [];
            foreach ($subsections as $subsection) {
                if (!is_array($subsection)) {
                    continue;
                }

                $subheading = note_renderer_safe_text((string)($subsection['subheading'] ?? ''));
                $subText = note_renderer_safe_text((string)($subsection['text'] ?? ''));
                if ($subheading !== '') {
                    $contentBlocks[] = ['type' => 'subheading', 'text' => $subheading];
                }
                if ($subText !== '') {
                    $contentBlocks[] = ['type' => 'paragraph', 'text' => $subText];
                }
            }
        }
    }

    $normalizedBlocks = [];
    foreach ($contentBlocks as $block) {
        if (!is_array($block)) {
            continue;
        }

        $typeRaw = strtolower(trim((string)($block['type'] ?? '')));
        $text = note_renderer_safe_text((string)($block['text'] ?? ''));
        $tagType = note_renderer_extract_structural_tag_type($text);
        $text = note_renderer_strip_structural_tag_prefix($text);

        $type = in_array($typeRaw, ['heading', 'subheading', 'paragraph', 'alpha_paragraph', 'quote'], true)
            ? $typeRaw
            : 'paragraph';
        if ($tagType !== null) {
            $type = $tagType;
        }
        if ($text === '') {
            continue;
        }
        if ($type === 'heading' || $type === 'subheading') {
            $text = note_renderer_strip_heading_number_prefix($text);
            if ($text === '') {
                continue;
            }
        }
        if ($type === 'alpha_paragraph') {
            $text = note_renderer_strip_alpha_prefix($text);
            if ($text === '') {
                continue;
            }
        }

        $normalizedBlocks[] = [
            'type' => $type,
            'text' => $text,
        ];
    }

    return [
        'meta' => [
            'project' => note_renderer_safe_text((string)($meta['project'] ?? '')),
            'title' => note_renderer_safe_text((string)($meta['title'] ?? '')),
            'file_reference' => note_renderer_safe_text((string)($meta['file_reference'] ?? '')),
        ],
        'content_blocks' => $normalizedBlocks,
    ];
}

function note_renderer_unique_output_path(string $dir, string $baseName, string $ext): string {
    $path = "{$dir}/{$baseName}.{$ext}";
    if (!file_exists($path)) {
        return $path;
    }

    $n = 2;
    do {
        $candidate = "{$dir}/{$baseName}_{$n}.{$ext}";
        if (!file_exists($candidate)) {
            return $candidate;
        }
        $n++;
    } while ($n < 1000);

    return "{$dir}/{$baseName}_" . date('His') . ".{$ext}";
}

function note_renderer_safe_filename_slug(string $value, string $fallback = 'note'): string {
    $text = strtolower(trim($value));
    if ($text === '') {
        return $fallback;
    }

    $text = preg_replace('/[^a-z0-9]+/i', '_', $text) ?? $text;
    $text = trim($text, '_');
    if ($text === '') {
        return $fallback;
    }

    return substr($text, 0, 120);
}

function note_renderer_paragraph_text_content(DOMElement $paragraph, DOMXPath $xp): string {
    $parts = [];
    $textNodes = $xp->query('.//w:t', $paragraph);
    if (!($textNodes instanceof DOMNodeList)) {
        return '';
    }

    foreach ($textNodes as $textNode) {
        $parts[] = $textNode->textContent;
    }

    return implode('', $parts);
}

function note_renderer_build_paragraph_xml(string $text, string $styleId): string {
    $styleSafe = htmlspecialchars($styleId, ENT_XML1 | ENT_QUOTES, 'UTF-8');
    $lineParts = preg_split('/\R/u', $text) ?: [$text];
    $runInner = '';
    foreach ($lineParts as $idx => $line) {
        if ($idx > 0) {
            $runInner .= '<w:br/>';
        }

        $lineSafe = htmlspecialchars((string)$line, ENT_XML1 | ENT_QUOTES, 'UTF-8');
        if ($lineSafe === '') {
            $runInner .= '<w:t xml:space="preserve"> </w:t>';
        } else {
            $runInner .= '<w:t xml:space="preserve">' . $lineSafe . '</w:t>';
        }
    }

    if ($runInner === '') {
        $runInner = '<w:t xml:space="preserve"> </w:t>';
    }

    return '<w:p xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        . '<w:pPr><w:pStyle w:val="' . $styleSafe . '"/></w:pPr>'
        . '<w:r>' . $runInner . '</w:r>'
        . '</w:p>';
}

function note_renderer_inject_blocks(string $docxPath, string $marker, array $blocks): void {
    $zip = new ZipArchive();
    if ($zip->open($docxPath) !== true) {
        throw new RuntimeException('Unable to open generated DOCX for post-processing.');
    }

    try {
        $xml = $zip->getFromName('word/document.xml');
        if (!is_string($xml) || $xml === '') {
            throw new RuntimeException('Generated DOCX has no word/document.xml.');
        }

        $dom = new DOMDocument('1.0', 'UTF-8');
        $dom->preserveWhiteSpace = true;
        $dom->formatOutput = false;
        if (!$dom->loadXML($xml, LIBXML_NONET | LIBXML_COMPACT)) {
            throw new RuntimeException('Could not parse generated document.xml.');
        }

        $xp = new DOMXPath($dom);
        $xp->registerNamespace('w', 'http://schemas.openxmlformats.org/wordprocessingml/2006/main');

        $paragraphs = $xp->query('//w:body/w:p');
        if (!($paragraphs instanceof DOMNodeList)) {
            throw new RuntimeException('Could not enumerate document paragraphs.');
        }

        $targetParagraph = null;
        $placeholderMacro = '${note_block}';
        foreach ($paragraphs as $paragraphNode) {
            if (!($paragraphNode instanceof DOMElement)) {
                continue;
            }

            $text = note_renderer_paragraph_text_content($paragraphNode, $xp);
            if ($text === $marker || $text === $placeholderMacro) {
                $targetParagraph = $paragraphNode;
                break;
            }
        }
        if (!($targetParagraph instanceof DOMElement)) {
            throw new RuntimeException('Could not locate note block marker in generated DOCX.');
        }

        $styleMap = [
            'heading' => 'Heading1',
            'subheading' => 'Heading2',
            'paragraph' => 'BodyText',
            'alpha_paragraph' => 'Numbering1',
            'quote' => 'Quote',
        ];

        $paragraphXml = '';
        foreach ($blocks as $block) {
            if (!is_array($block)) {
                continue;
            }

            $typeRaw = strtolower(trim((string)($block['type'] ?? 'paragraph')));
            $type = array_key_exists($typeRaw, $styleMap) ? $typeRaw : 'paragraph';
            $text = note_renderer_safe_text((string)($block['text'] ?? ''));
            if ($text === '') {
                continue;
            }

            $paragraphXml .= note_renderer_build_paragraph_xml($text, $styleMap[$type]);
        }
        if ($paragraphXml === '') {
            $paragraphXml = note_renderer_build_paragraph_xml('', $styleMap['paragraph']);
        }

        $fragment = $dom->createDocumentFragment();
        if (!$fragment->appendXML($paragraphXml)) {
            throw new RuntimeException('Could not construct generated note paragraphs.');
        }

        $parent = $targetParagraph->parentNode;
        if (!$parent) {
            throw new RuntimeException('Could not resolve marker paragraph parent.');
        }
        $parent->insertBefore($fragment, $targetParagraph);
        $parent->removeChild($targetParagraph);

        $updated = $dom->saveXML();
        if (!is_string($updated) || $updated === '') {
            throw new RuntimeException('Failed to serialize updated document.xml.');
        }
        if ($zip->addFromString('word/document.xml', $updated) === false) {
            throw new RuntimeException('Failed to write updated document.xml into DOCX.');
        }
    } finally {
        $zip->close();
    }
}

function note_renderer_render_word(array $result, array $options = []): array {
    $normalized = note_renderer_normalize_result($result);
    $meta = is_array($normalized['meta'] ?? null) ? $normalized['meta'] : [];
    $blocks = is_array($normalized['content_blocks'] ?? null) ? $normalized['content_blocks'] : [];

    $templatePath = (string)($options['template_path'] ?? (PUBLIC_PATH . 'test/note_template.docx'));
    if (!is_file($templatePath) || !is_readable($templatePath)) {
        throw new RuntimeException('Template file not found or unreadable.');
    }

    $outputDir = (string)($options['output_dir'] ?? (PUBLIC_PATH . 'upload_nmrk_app/generated_letters'));
    if (!is_dir($outputDir) && !mkdir($outputDir, 0777, true) && !is_dir($outputDir)) {
        throw new RuntimeException('Could not create output directory.');
    }

    $downloadBase = (string)($options['download_base'] ?? '/upload_nmrk_app/generated_letters/');
    $marker = (string)($options['marker'] ?? '__NOTE_BLOCK_MARKER__');

    $tpl = new \PhpOffice\PhpWord\TemplateProcessor($templatePath);
    $tpl->setValue('title', mb_strtoupper(note_renderer_safe_text((string)($meta['title'] ?? ''))));
    $tpl->setValue('date', date('d/m/y'));
    $tpl->setValue('job_no', note_renderer_safe_text((string)($meta['file_reference'] ?? '')));
    $tpl->setValue('note_block', $marker);

    $titleSlug = note_renderer_safe_filename_slug((string)($meta['title'] ?? ''), 'note');
    $outPath = note_renderer_unique_output_path($outputDir, date('ymd') . '_' . $titleSlug, 'docx');
    $tpl->saveAs($outPath);
    note_renderer_inject_blocks($outPath, $marker, $blocks);

    $filename = basename($outPath);
    return [
        'path' => $outPath,
        'file' => $filename,
        'downloadUrl' => rtrim($downloadBase, '/') . '/' . $filename,
        'normalized_result' => $normalized,
    ];
}
