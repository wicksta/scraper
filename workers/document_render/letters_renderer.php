<?php
declare(strict_types=1);

function letter_renderer_safe_text(mixed $value): string {
    if (!is_string($value)) {
        return '';
    }

    $text = str_replace(["\r\n", "\r"], "\n", $value);
    $text = preg_replace('/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/u', ' ', $text) ?? $text;
    $text = preg_replace('/\x{2028}|\x{2029}/u', "\n", $text) ?? $text;
    return trim($text);
}

function letter_renderer_strip_heading_number_prefix(string $value): string {
    $text = trim($value);
    if ($text === '') {
        return '';
    }

    $text = preg_replace('/^\s*\d+(?:\.\d+)*\s*[\.\)\-:]?\s*/u', '', $text) ?? $text;
    return trim($text);
}

function letter_renderer_strip_structural_tag_prefix(string $value): string {
    $text = trim($value);
    if ($text === '') {
        return '';
    }

    $text = preg_replace('/^\[(?:H1|H2|P|AP|Q)\]\s*/i', '', $text) ?? $text;
    return trim($text);
}

function letter_renderer_extract_structural_tag_type(string $value): ?string {
    if (!preg_match('/^\[(H1|H2|P|AP|Q)\]\s*/i', trim($value), $m)) {
        return null;
    }

    return match (strtoupper((string)$m[1])) {
        'H1' => 'heading',
        'H2' => 'subheading',
        'P' => 'paragraph',
        'AP' => 'bullet',
        'Q' => 'quote',
        default => null,
    };
}

function letter_renderer_strip_alpha_prefix(string $value): string {
    $text = trim($value);
    if ($text === '') {
        return '';
    }

    $text = preg_replace('/^\s*[a-z]\s*[\.\)\-:]\s*/i', '', $text) ?? $text;
    return trim($text);
}

function letter_renderer_normalize_result(array $decoded): array {
    $meta = is_array($decoded['meta'] ?? null) ? $decoded['meta'] : [];
    $contentBlocks = is_array($decoded['content_blocks'] ?? null) ? $decoded['content_blocks'] : [];

    if (count($contentBlocks) === 0) {
        $sections = is_array($decoded['sections'] ?? null) ? $decoded['sections'] : [];
        foreach ($sections as $section) {
            if (!is_array($section)) {
                continue;
            }

            $heading = letter_renderer_safe_text((string)($section['heading'] ?? ''));
            $text = letter_renderer_safe_text((string)($section['text'] ?? ''));
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

                $subheading = letter_renderer_safe_text((string)($subsection['subheading'] ?? ''));
                $subText = letter_renderer_safe_text((string)($subsection['text'] ?? ''));
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
        $text = letter_renderer_safe_text((string)($block['text'] ?? ''));
        $tagType = letter_renderer_extract_structural_tag_type($text);
        $text = letter_renderer_strip_structural_tag_prefix($text);
        if ($typeRaw === 'alpha_paragraph') {
            $typeRaw = 'bullet';
        }

        $type = in_array($typeRaw, ['heading', 'subheading', 'paragraph', 'bullet', 'quote'], true)
            ? $typeRaw
            : 'paragraph';
        if ($tagType !== null) {
            $type = $tagType;
        }
        if ($text === '') {
            continue;
        }
        if ($type === 'heading' || $type === 'subheading') {
            $text = letter_renderer_strip_heading_number_prefix($text);
            if ($text === '') {
                continue;
            }
        }
        if ($type === 'bullet') {
            $text = letter_renderer_strip_alpha_prefix($text);
            if ($text === '') {
                continue;
            }
        }

        $normalizedBlocks[] = [
            'type' => $type,
            'text' => $text,
        ];
    }

    $metaHeading = letter_renderer_safe_text((string)($meta['heading'] ?? ''));
    if ($metaHeading !== '' && $normalizedBlocks !== []) {
        $first = $normalizedBlocks[0];
        $firstType = strtolower(trim((string)($first['type'] ?? '')));
        $firstText = letter_renderer_safe_text((string)($first['text'] ?? ''));
        if ($firstType === 'heading' && strcasecmp($firstText, $metaHeading) === 0) {
            array_shift($normalizedBlocks);
        }
    }

    return [
        'meta' => [
            'address' => letter_renderer_safe_text((string)($meta['address'] ?? '')),
            'your_ref' => letter_renderer_safe_text((string)($meta['your_ref'] ?? '')),
            'our_ref' => letter_renderer_safe_text((string)($meta['our_ref'] ?? '')),
            'date' => letter_renderer_safe_text((string)($meta['date'] ?? '')),
            'heading' => letter_renderer_safe_text((string)($meta['heading'] ?? '')),
            'salutation' => letter_renderer_safe_text((string)($meta['salutation'] ?? '')),
            'sign_off' => letter_renderer_safe_text((string)($meta['sign_off'] ?? '')),
            'email' => letter_renderer_safe_text((string)($meta['email'] ?? '')),
            'phone_no' => letter_renderer_safe_text((string)($meta['phone_no'] ?? '')),
        ],
        'content_blocks' => $normalizedBlocks,
    ];
}

function letter_renderer_split_address(mixed $value): string {
    $text = letter_renderer_safe_text($value);
    if ($text === '') {
        return '';
    }
    if (str_contains($text, "\n")) {
        return $text;
    }

    $parts = preg_split('/\s*,\s*/u', $text) ?: [$text];
    $parts = array_values(array_filter(array_map(
        static fn ($part) => letter_renderer_safe_text((string)$part),
        $parts
    ), static fn ($part) => $part !== ''));

    return $parts === [] ? $text : implode("\n", $parts);
}

function letter_renderer_with_terminal_punctuation(string $text, string $suffix): string {
    $trimmed = rtrim($text);
    if ($trimmed === '') {
        return $suffix;
    }

    $trimmed = preg_replace('/[;,.:]+\s*and$/iu', '', $trimmed) ?? $trimmed;
    $trimmed = preg_replace('/\band$/iu', '', $trimmed) ?? $trimmed;
    $trimmed = preg_replace('/[;,.:]+$/u', '', $trimmed) ?? $trimmed;
    return rtrim($trimmed) . $suffix;
}

function letter_renderer_normalize_bullet_punctuation(array $blocks): array {
    $normalized = $blocks;
    $count = count($normalized);
    $i = 0;

    while ($i < $count) {
        $type = strtolower(trim((string)($normalized[$i]['type'] ?? '')));
        if ($type !== 'bullet') {
            $i++;
            continue;
        }

        $start = $i;
        while ($i < $count && strtolower(trim((string)($normalized[$i]['type'] ?? ''))) === 'bullet') {
            $i++;
        }

        $end = $i - 1;
        $runLength = $end - $start + 1;
        for ($j = $start; $j <= $end; $j++) {
            $text = letter_renderer_safe_text((string)($normalized[$j]['text'] ?? ''));
            if ($text === '') {
                continue;
            }

            $suffix = ';';
            if ($runLength === 1 || $j === $end) {
                $suffix = '.';
            } elseif ($j === $end - 1) {
                $suffix = '; and';
            }

            $normalized[$j]['text'] = letter_renderer_with_terminal_punctuation($text, $suffix);
        }
    }

    return $normalized;
}

function letter_renderer_normalize_blocks_for_render(array $blocks): array {
    $hasHeading = false;
    foreach ($blocks as $block) {
        if (!is_array($block)) {
            continue;
        }
        if (strtolower(trim((string)($block['type'] ?? ''))) === 'heading') {
            $hasHeading = true;
            break;
        }
    }

    $normalized = [];
    foreach ($blocks as $block) {
        if (!is_array($block)) {
            continue;
        }

        $copy = $block;
        if (!$hasHeading && strtolower(trim((string)($copy['type'] ?? ''))) === 'subheading') {
            $copy['type'] = 'heading';
        }
        $normalized[] = $copy;
    }

    return letter_renderer_normalize_bullet_punctuation($normalized);
}

function letter_renderer_render_date(mixed $value): string {
    $text = letter_renderer_safe_text($value);
    if ($text !== '') {
        return $text;
    }

    return (new DateTimeImmutable('now'))->format('j F Y');
}

function letter_renderer_logged_in_user_email(PDO $pdo): string {
    $userId = isset($_SESSION['user_id']) ? (int)$_SESSION['user_id'] : 0;
    if ($userId <= 0) {
        return '';
    }

    $stmt = $pdo->prepare('SELECT email FROM users WHERE id = ?');
    $stmt->execute([$userId]);
    $email = $stmt->fetchColumn();

    return is_string($email) ? letter_renderer_safe_text($email) : '';
}

function letter_renderer_sign_off(mixed $signOff, mixed $salutation): string {
    $explicit = letter_renderer_safe_text($signOff);
    if ($explicit !== '') {
        return $explicit;
    }

    $normalizedSalutation = strtolower(letter_renderer_safe_text($salutation));
    $normalizedSalutation = preg_replace('/[[:punct:]]+\s*$/u', '', $normalizedSalutation) ?? $normalizedSalutation;
    $normalizedSalutation = preg_replace('/\s+/u', ' ', trim($normalizedSalutation)) ?? trim($normalizedSalutation);

    if ($normalizedSalutation === '' || in_array($normalizedSalutation, ['dear sir', 'dear sirs', 'dear sir/madam', 'dear sir / madam'], true)) {
        return 'Yours faithfully';
    }

    return 'Yours sincerely';
}

function letter_renderer_unique_output_path(string $dir, string $baseName, string $ext): string {
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

function letter_renderer_safe_filename_slug(string $value, string $fallback = 'letter'): string {
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

function letter_renderer_paragraph_text_content(DOMElement $paragraph, DOMXPath $xp): string {
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

function letter_renderer_build_paragraph_xml(string $text, string $styleId): string {
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

function letter_renderer_inject_blocks(string $docxPath, string $marker, array $blocks): void {
    if (!class_exists('ZipArchive')) {
        throw new RuntimeException('ZipArchive is required to post-process DOCX files.');
    }

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
        $placeholderMacro = '${text}';
        foreach ($paragraphs as $paragraphNode) {
            if (!($paragraphNode instanceof DOMElement)) {
                continue;
            }

            $text = letter_renderer_paragraph_text_content($paragraphNode, $xp);
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
            'paragraph' => 'Normal',
            'bullet' => 'Bulletedtext',
            'quote' => 'Quote1',
        ];

        $paragraphXml = '';
        foreach ($blocks as $block) {
            if (!is_array($block)) {
                continue;
            }

            $typeRaw = strtolower(trim((string)($block['type'] ?? 'paragraph')));
            $type = array_key_exists($typeRaw, $styleMap) ? $typeRaw : 'paragraph';
            $text = letter_renderer_safe_text((string)($block['text'] ?? ''));
            if ($text === '') {
                continue;
            }

            $paragraphXml .= letter_renderer_build_paragraph_xml($text, $styleMap[$type]);
        }

        if ($paragraphXml === '') {
            $paragraphXml = letter_renderer_build_paragraph_xml('', $styleMap['paragraph']);
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

function letter_renderer_render_word(array $result, PDO $pdo, array $options = []): array {
    $normalized = letter_renderer_normalize_result($result);
    $meta = is_array($normalized['meta'] ?? null) ? $normalized['meta'] : [];
    $blocks = letter_renderer_normalize_blocks_for_render(is_array($normalized['content_blocks'] ?? null) ? $normalized['content_blocks'] : []);

    $templatePath = (string)($options['template_path'] ?? (PUBLIC_PATH . 'templates/letter_template.docx'));
    if (!is_file($templatePath) || !is_readable($templatePath)) {
        throw new RuntimeException('Template file not found or unreadable.');
    }

    $outputDir = (string)($options['output_dir'] ?? (PUBLIC_PATH . 'upload_nmrk_app/generated_letters'));
    if (!is_dir($outputDir) && !mkdir($outputDir, 0777, true) && !is_dir($outputDir)) {
        throw new RuntimeException('Could not create output directory.');
    }

    $downloadBase = (string)($options['download_base'] ?? '/upload_nmrk_app/generated_letters/');
    $marker = (string)($options['marker'] ?? '__LETTER_TEXT_MARKER__');

    $tpl = new \PhpOffice\PhpWord\TemplateProcessor($templatePath);
    $tpl->setValue('address', letter_renderer_split_address($meta['address'] ?? ''));
    $tpl->setValue('your_ref', letter_renderer_safe_text((string)($meta['your_ref'] ?? '')));
    $tpl->setValue('our_ref', letter_renderer_safe_text((string)($meta['our_ref'] ?? '')));
    $tpl->setValue('date', letter_renderer_render_date($meta['date'] ?? ''));
    $tpl->setValue('heading', letter_renderer_safe_text((string)($meta['heading'] ?? '')));
    $tpl->setValue('salutation', letter_renderer_safe_text((string)($meta['salutation'] ?? '')));
    $tpl->setValue('sign_off', letter_renderer_sign_off($meta['sign_off'] ?? '', $meta['salutation'] ?? ''));
    $tpl->setValue('email', letter_renderer_safe_text((string)($meta['email'] ?? '')) ?: letter_renderer_logged_in_user_email($pdo));
    $tpl->setValue('phone_no', letter_renderer_safe_text((string)($meta['phone_no'] ?? '')));
    $tpl->setValue('text', $marker);

    $titleSlug = letter_renderer_safe_filename_slug((string)($meta['heading'] ?? ''), 'letter');
    $base = date('ymd') . '_' . $titleSlug;
    $outPath = letter_renderer_unique_output_path($outputDir, $base, 'docx');
    $tpl->saveAs($outPath);
    letter_renderer_inject_blocks($outPath, $marker, $blocks);

    $filename = basename($outPath);
    return [
        'path' => $outPath,
        'file' => $filename,
        'downloadUrl' => rtrim($downloadBase, '/') . '/' . $filename,
        'normalized_result' => $normalized,
    ];
}
