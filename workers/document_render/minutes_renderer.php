<?php
declare(strict_types=1);

function minutes_renderer_safe_text(mixed $value): string {
    if (!is_string($value)) {
        return '';
    }

    $text = str_replace(["\r\n", "\r"], "\n", $value);
    $text = preg_replace('/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/u', ' ', $text) ?? $text;
    $text = preg_replace('/\x{2028}|\x{2029}/u', "\n", $text) ?? $text;
    return trim($text);
}

function minutes_renderer_replace_gerald_eve(string $value): string {
    return preg_replace('/\bgerald\s+eve\b/i', 'Newmark', $value) ?? $value;
}

function minutes_renderer_is_newmark_attendee(array $attendee): bool {
    $org = strtolower(trim((string)($attendee['org'] ?? '')));
    if ($org !== '' && str_contains($org, 'newmark')) {
        return true;
    }

    $name = strtolower(trim((string)($attendee['name'] ?? '')));
    if ($name !== '' && str_contains($name, 'newmark')) {
        return true;
    }

    return false;
}

function minutes_renderer_attendee_sort_key(array $attendee): string {
    $name = trim((string)($attendee['name'] ?? ''));
    if ($name === '') {
        return '';
    }

    if (str_contains($name, ',')) {
        $parts = explode(',', $name, 2);
        $surname = trim((string)($parts[0] ?? ''));
    } else {
        $parts = preg_split('/\s+/', $name) ?: [];
        $surname = trim((string)(end($parts) ?: ''));
    }

    $normalized = preg_replace("/[^a-z0-9'\\-]+/i", '', $surname) ?? $surname;
    $normalized = strtolower(trim($normalized));

    return $normalized !== '' ? $normalized : strtolower($name);
}

function minutes_renderer_format_location(string $value): string {
    $raw = trim($value);
    if ($raw === '') {
        return '';
    }

    $normalized = strtolower(preg_replace('/[^a-z0-9]+/', '', $raw) ?? $raw);
    return match ($normalized) {
        'teams' => 'Teams',
        'msteams', 'microsoftteams' => 'MS Teams',
        'zoom' => 'Zoom',
        'online' => 'Online',
        default => 'At ' . $raw,
    };
}

function minutes_renderer_unique_output_path(string $dir, string $baseName, string $ext): string {
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

function minutes_renderer_enforce_table_gap(string $docxPath, string $marker): void {
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
        $markerTextNodes = $xp->query('//w:t[text()="' . $marker . '"]');
        if (!($markerTextNodes instanceof DOMNodeList) || $markerTextNodes->length < 1) {
            throw new RuntimeException('Could not locate table gap marker in generated DOCX.');
        }

        $targetParagraph = null;
        foreach ($markerTextNodes as $textNode) {
            $candidate = $textNode;
            while ($candidate !== null && $candidate->nodeName !== 'w:p') {
                $candidate = $candidate->parentNode;
            }
            if ($candidate instanceof DOMElement && $candidate->nodeName === 'w:p') {
                $targetParagraph = $candidate;
                break;
            }
        }
        if (!($targetParagraph instanceof DOMElement)) {
            throw new RuntimeException('Found marker text but could not resolve containing paragraph.');
        }

        $spacerXml = '<w:p xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:pPr><w:spacing w:before="0" w:after="0" w:line="240" w:lineRule="auto"/></w:pPr><w:r><w:t xml:space="preserve"> </w:t></w:r></w:p>';
        $spacerA = $dom->createDocumentFragment();
        $spacerB = $dom->createDocumentFragment();
        if (!$spacerA->appendXML($spacerXml) || !$spacerB->appendXML($spacerXml)) {
            throw new RuntimeException('Could not construct spacer paragraphs.');
        }

        $parent = $targetParagraph->parentNode;
        if (!$parent) {
            throw new RuntimeException('Could not resolve paragraph parent node.');
        }

        $parent->insertBefore($spacerA, $targetParagraph);
        $parent->insertBefore($spacerB, $targetParagraph);
        $parent->removeChild($targetParagraph);

        $floatingTableProps = $xp->query('//w:tblPr/w:tblpPr');
        if ($floatingTableProps instanceof DOMNodeList) {
            foreach ($floatingTableProps as $node) {
                $nodeParent = $node->parentNode;
                if ($nodeParent) {
                    $nodeParent->removeChild($node);
                }
            }
        }

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

function minutes_renderer_format_caps_date(string $value): string {
    $raw = trim($value);
    if ($raw === '') {
        return '';
    }

    $dt = null;
    foreach (['Y-m-d', 'd/m/Y', 'j/n/Y', 'd-m-Y', 'j-n-Y'] as $fmt) {
        $try = DateTime::createFromFormat($fmt, $raw);
        if ($try instanceof DateTime) {
            $dt = $try;
            break;
        }
    }
    if (!$dt) {
        $ts = strtotime($raw);
        if ($ts !== false) {
            $dt = (new DateTime())->setTimestamp($ts);
        }
    }

    return $dt ? mb_strtoupper($dt->format('j F Y')) : mb_strtoupper($raw);
}

function minutes_renderer_parse_flexible_date(string $value): ?DateTimeImmutable {
    $raw = trim($value);
    if ($raw === '') {
        return null;
    }

    foreach (['Y-m-d', 'd/m/Y', 'j/n/Y', 'd-m-Y', 'j-n-Y'] as $fmt) {
        $try = DateTimeImmutable::createFromFormat($fmt, $raw);
        if ($try instanceof DateTimeImmutable) {
            return $try->setTime(0, 0, 0);
        }
    }

    $ts = strtotime($raw);
    if ($ts === false) {
        return null;
    }

    return (new DateTimeImmutable())->setTimestamp($ts)->setTime(0, 0, 0);
}

function minutes_renderer_normalize_date(string $value): string {
    $today = new DateTimeImmutable('today');
    $raw = trim($value);
    if ($raw === '') {
        return $today->format('Y-m-d');
    }

    $parsed = minutes_renderer_parse_flexible_date($raw);
    if (!$parsed) {
        return $today->format('Y-m-d');
    }

    $cutoff = $today->sub(new DateInterval('P6M'));
    if ($parsed < $cutoff) {
        return $today->format('Y-m-d');
    }

    return $parsed->format('Y-m-d');
}

function minutes_renderer_normalize_result(array $result): array {
    $meta = is_array($result['meta'] ?? null) ? $result['meta'] : [];
    $attendeesRaw = is_array($result['attendees'] ?? null) ? $result['attendees'] : [];
    $minutesRaw = is_array($result['minutes'] ?? null) ? $result['minutes'] : [];

    $attendees = [];
    foreach ($attendeesRaw as $row) {
        if (!is_array($row)) {
            continue;
        }
        $attendees[] = [
            'name' => minutes_renderer_safe_text((string)($row['name'] ?? '')),
            'initials' => minutes_renderer_safe_text((string)($row['initials'] ?? '')),
            'organisation' => minutes_renderer_safe_text((string)($row['organisation'] ?? '')),
        ];
    }

    $minutes = [];
    foreach ($minutesRaw as $row) {
        if (!is_array($row)) {
            continue;
        }
        $minutes[] = [
            'text' => minutes_renderer_safe_text((string)($row['text'] ?? '')),
            'action' => minutes_renderer_safe_text((string)($row['action'] ?? '')),
        ];
    }

    return [
        'meta' => [
            'title' => minutes_renderer_safe_text((string)($meta['title'] ?? '')),
            'date' => minutes_renderer_normalize_date((string)($meta['date'] ?? '')),
            'time' => minutes_renderer_safe_text((string)($meta['time'] ?? '')),
            'job_code' => minutes_renderer_safe_text((string)($meta['job_code'] ?? '')),
            'location' => minutes_renderer_safe_text((string)($meta['location'] ?? '')),
            'project' => minutes_renderer_safe_text((string)($meta['project'] ?? '')),
        ],
        'attendees' => $attendees,
        'minutes' => $minutes,
    ];
}

function minutes_renderer_render_word(array $result, array $options = []): array {
    $normalized = minutes_renderer_normalize_result($result);
    $meta = is_array($normalized['meta'] ?? null) ? $normalized['meta'] : [];
    $attendees = is_array($normalized['attendees'] ?? null) ? $normalized['attendees'] : [];
    $minutes = is_array($normalized['minutes'] ?? null) ? $normalized['minutes'] : [];

    $templatePath = (string)($options['template_path'] ?? (PUBLIC_PATH . 'test/minutes_template.docx'));
    if (!is_file($templatePath) || !is_readable($templatePath)) {
        throw new RuntimeException('Template file not found or unreadable.');
    }

    $outputDir = (string)($options['output_dir'] ?? (PUBLIC_PATH . 'upload_nmrk_app/generated_letters'));
    if (!is_dir($outputDir) && !mkdir($outputDir, 0777, true) && !is_dir($outputDir)) {
        throw new RuntimeException('Could not create output directory.');
    }

    $downloadBase = (string)($options['download_base'] ?? '/upload_nmrk_app/generated_letters/');
    $tableGapMarker = (string)($options['table_gap_marker'] ?? '__TABLE_GAP_MARKER__');

    if (method_exists('\\PhpOffice\\PhpWord\\Settings', 'setOutputEscapingEnabled')) {
        \PhpOffice\PhpWord\Settings::setOutputEscapingEnabled(true);
    }

    $tpl = new \PhpOffice\PhpWord\TemplateProcessor($templatePath);

    $dateRaw = minutes_renderer_normalize_date((string)($meta['date'] ?? ''));
    $titleValue = minutes_renderer_safe_text((string)($meta['title'] ?? ''));
    $projectValue = minutes_renderer_safe_text((string)($meta['project'] ?? ''));
    if ($projectValue === '') {
        $projectValue = $titleValue;
    }
    if ($titleValue === '') {
        $titleValue = $projectValue;
    }

    $tpl->setValue('date', minutes_renderer_format_caps_date($dateRaw));
    $tpl->setValue('date_of_minutes', date('d/m/y'));
    $tpl->setValue('project', $projectValue);
    $tpl->setValue('location', minutes_renderer_format_location((string)($meta['location'] ?? '')));
    $tpl->setValue('job_no', minutes_renderer_safe_text((string)($meta['job_code'] ?? '')));
    $tpl->setValue('title', $titleValue);
    $tpl->setValue('time', minutes_renderer_safe_text((string)($meta['time'] ?? '')));

    $attendeeRows = [];
    foreach ($attendees as $row) {
        if (!is_array($row)) {
            continue;
        }
        $attendeeRows[] = [
            'name' => minutes_renderer_replace_gerald_eve(minutes_renderer_safe_text((string)($row['name'] ?? ''))),
            'initials' => minutes_renderer_replace_gerald_eve(minutes_renderer_safe_text((string)($row['initials'] ?? ''))),
            'org' => minutes_renderer_replace_gerald_eve(minutes_renderer_safe_text((string)($row['organisation'] ?? ''))),
        ];
    }
    if (count($attendeeRows) > 1) {
        usort($attendeeRows, static function (array $a, array $b): int {
            $aIsNewmark = minutes_renderer_is_newmark_attendee($a);
            $bIsNewmark = minutes_renderer_is_newmark_attendee($b);
            if ($aIsNewmark !== $bIsNewmark) {
                return $aIsNewmark ? 1 : -1;
            }

            $surnameCompare = strcmp(minutes_renderer_attendee_sort_key($a), minutes_renderer_attendee_sort_key($b));
            if ($surnameCompare !== 0) {
                return $surnameCompare;
            }

            return strcasecmp((string)($a['name'] ?? ''), (string)($b['name'] ?? ''));
        });
    }
    if (count($attendeeRows) === 0) {
        $attendeeRows[] = ['name' => '', 'initials' => '', 'org' => ''];
    }

    $firstAttendee = $attendeeRows[0];
    $tpl->setValue('name_first', $firstAttendee['name']);
    $tpl->setValue('initials_first', $firstAttendee['initials']);
    $tpl->setValue('org_first', $firstAttendee['org']);

    $remainingAttendeeRows = array_slice($attendeeRows, 1);
    if (count($remainingAttendeeRows) === 0) {
        $remainingAttendeeRows[] = ['name' => '', 'initials' => '', 'org' => ''];
    }

    $tpl->cloneRow('name', count($remainingAttendeeRows));
    foreach ($remainingAttendeeRows as $idx => $row) {
        $n = $idx + 1;
        $tpl->setValue("name#{$n}", $row['name']);
        $tpl->setValue("initial#{$n}", $row['initials']);
        $tpl->setValue("initials#{$n}", $row['initials']);
        $tpl->setValue("org#{$n}", $row['org']);
    }
    $tpl->setValue('table_gap', $tableGapMarker);

    $minuteRows = [];
    foreach ($minutes as $row) {
        if (!is_array($row)) {
            continue;
        }
        $minuteRows[] = [
            'minute' => minutes_renderer_safe_text((string)($row['text'] ?? '')),
            'action' => minutes_renderer_safe_text((string)($row['action'] ?? '')),
        ];
    }
    if (count($minuteRows) === 0) {
        $minuteRows[] = ['minute' => '', 'action' => ''];
    }

    $tpl->cloneRow('minute', count($minuteRows));
    foreach ($minuteRows as $idx => $row) {
        $n = $idx + 1;
        $tpl->setValue("no#{$n}", (string)$n);
        $tpl->setValue("minute#{$n}", $row['minute']);
        $tpl->setValue("action#{$n}", $row['action']);
        $tpl->setValue("action{#{$n}", $row['action']);
    }

    $outPath = minutes_renderer_unique_output_path($outputDir, 'minutes_' . date('Ymd_His'), 'docx');
    $tpl->saveAs($outPath);
    minutes_renderer_enforce_table_gap($outPath, $tableGapMarker);

    $filename = basename($outPath);
    return [
        'path' => $outPath,
        'file' => $filename,
        'downloadUrl' => rtrim($downloadBase, '/') . '/' . $filename,
        'normalized_result' => $normalized,
    ];
}
