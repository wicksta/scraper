<?php
declare(strict_types=1);

const DOCGEN_LETTER_MODEL_DEFAULT = 'gpt-5-mini';
const DOCGEN_NOTE_MODEL_DEFAULT = 'gpt-5-mini';
const DOCGEN_MINUTES_MODEL_DEFAULT = 'gpt-5-mini';
const DOCGEN_MAX_CHARS = 60000;

function docgen_strip_markdown_fences(string $text): string
{
    $trimmed = trim($text);
    $trimmed = preg_replace('/^```(?:json)?\s*/i', '', $trimmed) ?? $trimmed;
    $trimmed = preg_replace('/\s*```$/', '', $trimmed) ?? $trimmed;
    return trim($trimmed);
}

function docgen_safe_text(mixed $value): string
{
    if (!is_string($value)) {
        return '';
    }
    $text = str_replace(["\r\n", "\r"], "\n", $value);
    $text = preg_replace('/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/u', ' ', $text) ?? $text;
    $text = preg_replace('/\x{2028}|\x{2029}/u', "\n", $text) ?? $text;
    return trim($text);
}

function docgen_strip_heading_number_prefix(string $value): string
{
    $text = trim($value);
    if ($text === '') {
        return '';
    }
    $text = preg_replace('/^\s*\d+(?:\.\d+)*\s*[\.\)\-:]?\s*/u', '', $text) ?? $text;
    return trim($text);
}

function docgen_strip_structural_tag_prefix(string $value): string
{
    $text = trim($value);
    if ($text === '') {
        return '';
    }
    $text = preg_replace('/^\[(?:H1|H2|P|AP|Q)\]\s*/i', '', $text) ?? $text;
    return trim($text);
}

function docgen_extract_structural_tag_type(string $value, string $docType): ?string
{
    if (!preg_match('/^\[(H1|H2|P|AP|Q)\]\s*/i', trim($value), $m)) {
        return null;
    }

    return match (strtoupper((string) $m[1])) {
        'H1' => 'heading',
        'H2' => 'subheading',
        'P' => 'paragraph',
        'AP' => $docType === 'letter' ? 'bullet' : 'alpha_paragraph',
        'Q' => 'quote',
        default => null,
    };
}

function docgen_strip_alpha_prefix(string $value): string
{
    $text = trim($value);
    if ($text === '') {
        return '';
    }
    $text = preg_replace('/^\s*[a-z]\s*[\.\)\-:]\s*/i', '', $text) ?? $text;
    return trim($text);
}

function docgen_read_docx_text(string $tmpPath): string
{
    $zip = new ZipArchive();
    if ($zip->open($tmpPath) !== true) {
        throw new RuntimeException('Unable to open DOCX file.');
    }

    $xml = $zip->getFromName('word/document.xml');
    $zip->close();
    if (!is_string($xml) || $xml === '') {
        throw new RuntimeException('DOCX appears to have no readable document.xml content.');
    }

    $xml = preg_replace('/<\/w:p>/', "\n", $xml) ?? $xml;
    $xml = preg_replace('/<[^>]+>/', ' ', $xml) ?? $xml;
    $xml = html_entity_decode($xml, ENT_QUOTES | ENT_XML1, 'UTF-8');
    $xml = preg_replace('/[ \t]+/', ' ', $xml) ?? $xml;
    $xml = preg_replace('/\n{3,}/', "\n\n", $xml) ?? $xml;
    return trim((string) $xml);
}

function docgen_call_openai_json(string $systemPrompt, string $userPrompt, string $model): array
{
    if (!defined('OPENAI_API_KEY') || OPENAI_API_KEY === '') {
        return ['ok' => false, 'error' => 'OPENAI_API_KEY is missing.'];
    }

    $payload = [
        'model' => $model,
        'response_format' => ['type' => 'json_object'],
        'messages' => [
            ['role' => 'system', 'content' => $systemPrompt],
            ['role' => 'user', 'content' => $userPrompt],
        ],
    ];

    $ch = curl_init('https://api.openai.com/v1/chat/completions');
    curl_setopt_array($ch, [
        CURLOPT_RETURNTRANSFER => true,
        CURLOPT_POST => true,
        CURLOPT_HTTPHEADER => [
            'Content-Type: application/json',
            'Authorization: Bearer ' . OPENAI_API_KEY,
        ],
        CURLOPT_POSTFIELDS => json_encode($payload, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES),
        CURLOPT_TIMEOUT => 120,
    ]);

    $raw = curl_exec($ch);
    $status = (int) curl_getinfo($ch, CURLINFO_HTTP_CODE);
    $err = curl_error($ch);
    curl_close($ch);

    if ($raw === false) {
        return ['ok' => false, 'error' => 'OpenAI cURL error: ' . $err];
    }

    $json = json_decode((string) $raw, true);
    if (!is_array($json)) {
        return ['ok' => false, 'error' => 'OpenAI returned non-JSON response.', 'status' => $status];
    }
    if ($status >= 400 || isset($json['error'])) {
        return ['ok' => false, 'error' => 'OpenAI API error.', 'status' => $status, 'details' => $json];
    }

    $content = $json['choices'][0]['message']['content'] ?? '';
    if (!is_string($content) || trim($content) === '') {
        return ['ok' => false, 'error' => 'OpenAI response missing assistant content.', 'details' => $json];
    }

    $decoded = json_decode(docgen_strip_markdown_fences($content), true);
    if (!is_array($decoded)) {
        return ['ok' => false, 'error' => 'Assistant output was not valid JSON.', 'assistant_raw' => $content];
    }

    return [
        'ok' => true,
        'assistant_json' => $decoded,
        'model_returned' => $json['model'] ?? $model,
        'usage' => $json['usage'] ?? null,
    ];
}

function docgen_prompt_config(string $docType, string $notes): array
{
    return match ($docType) {
        'letter' => [
            'model' => DOCGEN_LETTER_MODEL_DEFAULT,
            'system' => 'You convert draft planning letter text into structured JSON. Return only valid JSON.',
            'user' => <<<PROMPT
You are given draft planning letter text. Convert it into structured JSON.
Do not invent facts. Preserve source order and source text. Use null for missing scalar values and [] for missing arrays.
Do not summarise or abbreviate the substance of the draft. Keep the wording as close as possible to the source text, while correcting obvious typos and spelling mistakes.
Your job is to improve structure and formatting, not to shorten content.
First, identify any title-like, heading-like, subject-line-like, or metadata-like lines in the source.
If a line is clearly functioning as a title, heading, subject line, address heading, or other metadata/header text, extract it into the appropriate metadata field rather than forcing it to remain as an ordinary body paragraph.
Before preserving body paragraphs, identify title-like or metadata-like opening lines such as address block, heading, references, salutation, and contact details.
Extract those into metadata fields where appropriate instead of treating them as ordinary body paragraphs.
Treat each remaining source body paragraph as sacred.
Every substantive body paragraph from the source should appear in the output in the same order, except for explicit sign-off text that the template handles separately.
Do not compress multiple body paragraphs into one shorter rewritten paragraph.
When in doubt, copy the source wording more literally rather than rewriting it.
This rule applies to body content, not to title/header/metadata lines that should be extracted separately.
Represent content in reading order as blocks so multiple paragraphs are preserved.
Extract or infer these metadata values where possible: address, your_ref, our_ref, heading, salutation, email, phone_no.
If a value is missing, use null.
Do not include manual numbering prefixes in heading/subheading text (for example "1.", "2)", "3.1").
Heading numbering is applied automatically by the Word document styles.
If the notes contain tags like [H1], [H2], [P], [AP], and [Q], treat them as strong structural hints:
- [H1] => heading
- [H2] => subheading
- [P] => paragraph
- [AP] => bullet
- [Q] => quote
Do not include these tags in the final text output.
If the letter text you are given includes the sign-off (eg Yours sinverely / yours faithfully, and Newmark), omit them from the content you pass back, the Word template will sort them automatically.

Field Definitions:
address - should include the whole address, including a person or individual at the start of the address.  If you find more than one address, then prioritise one at the start of the document.

heading - be willing to include line breaks.  For example, if you are passed 10 Downing Street, London, SW1 2AA, Town and Country Planning Act 1990 (as amended), break between the address and the Act.

our_ref - If a job number/file number is present, put it in meta.file_reference.
Typical formats include examples like:
- JCW/SP2
- JCW/LDA/SP2
- JCW/J5152
- JCW/NLA/J5152
- JCW/NLA/U0012345

Return only JSON with this exact top-level shape:
{
  "meta": {
    "address": string|null,
    "your_ref": string|null,
    "our_ref": string|null,
    "heading": string|null,
    "salutation": string|null,
    "email": string|null,
    "phone_no": string|null
  },
  "content_blocks": [
    {
      "type": "heading"|"subheading"|"paragraph"|"bullet"|"quote",
      "text": string|null
    }
  ]
}
Draft notes:
{$notes}
PROMPT,
        ],
        'note' => [
            'model' => DOCGEN_NOTE_MODEL_DEFAULT,
            'system' => 'You convert draft Advice Note text into structured JSON. Return only valid JSON.',
            'user' => <<<PROMPT
You are given text to be turned into a draft advice note. Convert them into structured JSON.
Do not invent facts. Preserve source order and source text. Use null for missing scalar values and [] for missing arrays.
Do not summarise or abbreviate the substance of the draft. Keep the wording as close as possible to the source text, while correcting obvious typos and spelling mistakes.
Your job is to improve structure and formatting, not to shorten content.
First, identify any title-like, heading-like, subject-line-like, or metadata-like lines in the source.
If a line is clearly functioning as a title, heading, subject line, or other metadata/header text, extract it into the appropriate metadata field rather than forcing it to remain as an ordinary body paragraph.
Before preserving body paragraphs, identify whether the source contains a clear title or heading.
If a clear title is present, put it in meta.title and do not also repeat it as an ordinary paragraph unless the source clearly intends it to appear in the body as well.
If the source begins with a short standalone line that looks like a document title, use that as meta.title.
If, later in the source, there are short standalone lines separated from surrounding text that clearly function as section headings, represent them as content_blocks with type "heading" rather than absorbing them into neighbouring paragraphs.
A short standalone line is especially likely to be a heading if it:
- is followed by one or more explanatory paragraphs
- is separated by blank lines
- is much shorter than the surrounding paragraphs
- does not read like a complete body paragraph
- names a topic, team, workstream, project, or issue
Treat each remaining source body paragraph as sacred.
Every substantive body paragraph from the source should appear in the output in the same order.
Do not compress multiple body paragraphs into one shorter rewritten paragraph.
When in doubt, copy the source wording more literally rather than rewriting it.
This rule applies to body content, not to title/header/metadata lines that should be extracted separately.
Represent content in reading order as blocks so multiple paragraphs are preserved.
If a job number/file number is present, put it in meta.file_reference.
Typical formats include examples like:
- JCW/SP2
- JCW/LDA/SP2
- JCW/J5152
- JCW/NLA/J5152
- JCW/NLA/U0012345
If a clear title is present in the source, use that exact title or a very close normalised version.
Only create a new title if there is genuinely no clear title in the source.
Do not merge multiple headings together into a single invented title.
If no clear title is present in the notes, create a concise, sensible title and put it in meta.title.
Do not include manual numbering prefixes in heading/subheading text (for example "1.", "2)", "3.1").
Heading numbering is applied automatically by the Word document styles.
Do not use subheadings unless subheadings are specifically requested or explicitly indicated in the source text.
If the notes contain tags like [H1], [H2], [P], [AP], and [Q], treat them as strong structural hints:
- [H1] => heading
- [H2] => subheading
- [P] => paragraph
- [AP] => alpha_paragraph (lettered paragraph, e.g. a., b., c.)
- [Q] => quote
Do not include these tags in the final text output.

Return only JSON with this exact top-level shape:
{
  "meta": {
    "project": string|null,
    "title": string|null,
    "file_reference": string|null
  },
  "content_blocks": [
    {
      "type": "heading"|"subheading"|"paragraph"|"alpha_paragraph"|"quote",
      "text": string|null
    }
  ]
}
Draft notes:
{$notes}
PROMPT,
        ],
        'minutes' => [
            'model' => DOCGEN_MINUTES_MODEL_DEFAULT,
            'system' => 'You are a secretary. Extract structured meeting minutes from draft notes and return only valid JSON.',
            'user' => <<<PROMPT
You are a secretary. You have been given draft notes of a meeting.
First identify the meeting title, date, attendees, and any other obvious meeting metadata.
If the source contains a title-like line or subject-style heading, use that for meta.title rather than treating it as an ordinary minute paragraph.
Then extract the minutes. If there is an obvious action noted on someone, note their name in the minutes.action field.
Do not summarise or abbreviate the substance of the notes. Keep the minute text as close as possible to the source wording, while correcting obvious typos and spelling mistakes.
Your job is to improve structure and formatting, not to shorten content.
Treat each remaining source paragraph or note item as sacred.
Every substantive paragraph or note item from the source should appear in the output minutes in the same order.
Do not compress several body points into a shorter summary item.
When in doubt, copy the source wording more literally rather than rewriting it.
This rule applies to body content, not to title/header/metadata lines that should be extracted separately.
Where possible, use initials rather than people's names in the minute text. Do not worry if there is no obvious action - NULL is also acceptable.
Return only JSON with this exact shape:
{
  "meta": {
    "title": string|null,
    "date": string|null,
    "time": string|null,
    "job_code": string|null,
    "location": string|null,
    "project": string|null
  },
  "attendees": [
    {
      "name": string|null,
      "initials": string|null,
      "organisation": string|null
    }
  ],
  "minutes": [
    {
      "text": string|null,
      "action": string|null
    }
  ]
}

Draft notes:
{$notes}
PROMPT,
        ],
        default => throw new InvalidArgumentException('Unsupported document type.'),
    };
}

function docgen_normalize_content_blocks(array $contentBlocks, string $docType): array
{
    $allowed = match ($docType) {
        'letter' => ['heading', 'subheading', 'paragraph', 'bullet', 'quote'],
        'note' => ['heading', 'subheading', 'paragraph', 'alpha_paragraph', 'quote'],
        default => ['paragraph'],
    };

    $normalized = [];
    foreach ($contentBlocks as $block) {
        if (!is_array($block)) {
            continue;
        }
        $typeRaw = strtolower(trim((string) ($block['type'] ?? '')));
        $text = docgen_safe_text((string) ($block['text'] ?? ''));
        $tagType = docgen_extract_structural_tag_type($text, $docType);
        $text = docgen_strip_structural_tag_prefix($text);

        if ($docType === 'letter' && $typeRaw === 'alpha_paragraph') {
            $typeRaw = 'bullet';
        }

        $type = in_array($typeRaw, $allowed, true) ? $typeRaw : 'paragraph';
        if ($tagType !== null) {
            $type = $tagType;
        }
        if ($text === '') {
            continue;
        }
        if ($type === 'heading' || $type === 'subheading') {
            $text = docgen_strip_heading_number_prefix($text);
        }
        if ($type === 'bullet' || $type === 'alpha_paragraph') {
            $text = docgen_strip_alpha_prefix($text);
        }
        if ($text === '') {
            continue;
        }
        $normalized[] = ['type' => $type, 'text' => $text];
    }

    return $normalized;
}

function docgen_normalize_result(string $docType, array $decoded): array
{
    $meta = is_array($decoded['meta'] ?? null) ? $decoded['meta'] : [];
    $contentBlocks = is_array($decoded['content_blocks'] ?? null) ? $decoded['content_blocks'] : [];

    if ($docType === 'minutes') {
        $attendeesRaw = is_array($decoded['attendees'] ?? null) ? $decoded['attendees'] : [];
        $minutesRaw = is_array($decoded['minutes'] ?? null) ? $decoded['minutes'] : [];
        $attendees = [];
        foreach ($attendeesRaw as $row) {
            if (!is_array($row)) {
                continue;
            }
            $attendees[] = [
                'name' => docgen_safe_text((string) ($row['name'] ?? '')),
                'initials' => docgen_safe_text((string) ($row['initials'] ?? '')),
                'organisation' => docgen_safe_text((string) ($row['organisation'] ?? '')),
            ];
        }
        $minutes = [];
        foreach ($minutesRaw as $row) {
            if (!is_array($row)) {
                continue;
            }
            $minutes[] = [
                'text' => docgen_safe_text((string) ($row['text'] ?? '')),
                'action' => docgen_safe_text((string) ($row['action'] ?? '')),
            ];
        }

        return [
            'meta' => [
                'title' => docgen_safe_text((string) ($meta['title'] ?? '')),
                'date' => docgen_safe_text((string) ($meta['date'] ?? '')),
                'time' => docgen_safe_text((string) ($meta['time'] ?? '')),
                'job_code' => docgen_safe_text((string) ($meta['job_code'] ?? '')),
                'location' => docgen_safe_text((string) ($meta['location'] ?? '')),
                'project' => docgen_safe_text((string) ($meta['project'] ?? '')),
            ],
            'attendees' => $attendees,
            'minutes' => $minutes,
        ];
    }

    $normalized = [
        'meta' => [],
        'content_blocks' => docgen_normalize_content_blocks($contentBlocks, $docType),
    ];

    if ($docType === 'letter') {
        $normalized['meta'] = [
            'address' => docgen_safe_text((string) ($meta['address'] ?? '')),
            'your_ref' => docgen_safe_text((string) ($meta['your_ref'] ?? '')),
            'our_ref' => docgen_safe_text((string) ($meta['our_ref'] ?? '')),
            'date' => docgen_safe_text((string) ($meta['date'] ?? '')),
            'heading' => docgen_safe_text((string) ($meta['heading'] ?? '')),
            'salutation' => docgen_safe_text((string) ($meta['salutation'] ?? '')),
            'sign_off' => docgen_safe_text((string) ($meta['sign_off'] ?? '')),
            'email' => docgen_safe_text((string) ($meta['email'] ?? '')),
            'phone_no' => docgen_safe_text((string) ($meta['phone_no'] ?? '')),
        ];
    } else {
        $normalized['meta'] = [
            'project' => docgen_safe_text((string) ($meta['project'] ?? '')),
            'title' => docgen_safe_text((string) ($meta['title'] ?? '')),
            'file_reference' => docgen_safe_text((string) ($meta['file_reference'] ?? '')),
        ];
    }

    return $normalized;
}

function docgen_generate_structured(string $docType, string $notes, ?string $source = null): array
{
    $docType = strtolower(trim($docType));
    if (!in_array($docType, ['note', 'letter', 'minutes'], true)) {
        throw new InvalidArgumentException('Invalid or missing doc_type.');
    }

    $notes = trim($notes);
    if ($notes === '') {
        throw new InvalidArgumentException('Provide draft notes text or upload a .docx file.');
    }

    $notesForModel = mb_substr($notes, 0, DOCGEN_MAX_CHARS);
    $cfg = docgen_prompt_config($docType, $notesForModel);
    $ai = docgen_call_openai_json($cfg['system'], $cfg['user'], $cfg['model']);
    if (!($ai['ok'] ?? false)) {
        throw new RuntimeException('AI generation failed.', 0, new RuntimeException(json_encode($ai, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) ?: ''));
    }

    $assistantJson = is_array($ai['assistant_json'] ?? null) ? $ai['assistant_json'] : [];

    return [
        'success' => true,
        'doc_type' => $docType,
        'source' => $source,
        'notes_chars' => mb_strlen($notes),
        'notes_sent_chars' => mb_strlen($notesForModel),
        'assistant_json' => $assistantJson,
        'result' => docgen_normalize_result($docType, $assistantJson),
        'openai_meta' => [
            'model_requested' => $cfg['model'],
            'model_returned' => $ai['model_returned'] ?? $cfg['model'],
            'usage' => $ai['usage'] ?? null,
        ],
    ];
}
