<?php
declare(strict_types=1);

//require_once __DIR__ . '/../../../public_html/connect.php';              // $pdo (MySQL), $pg (Postgres)
//require_once __DIR__ . '/../../../public_html/vendor/autoload.php';              // $pdo (MySQL), $pg (Postgres)

// Usage (CLI):
//   php cttee_scraper.php                # all meetings
//   php cttee_scraper.php '?limit=3'     # first 3 meetings
//   php cttee_scraper.php '?url=FULL_MEETING_DOCS_URL'  # single meeting test
//
// Output TSV:
// meeting_date  meeting_title  application_title  report_url  meeting_docs_url



function meeting_extractor_tool(): array {
    return [[
        'type' => 'function',
        'function' => [
            'name' => 'extract_meeting',
            'description' => 'Extract meeting date and application report PDF links from a committee meeting page.',
            'parameters' => [
                'type' => 'object',
                'properties' => [
                    'meeting_date' => [
                        'type' => ['string','null'],
                        'description' => 'Meeting date as shown on the page (prefer long form, e.g., "10 June 2025").'
                    ],
                    'apps' => [
                        'type' => 'array',
                        'items' => [
                            'type' => 'object',
                            'properties' => [
                                'title' => ['type' => 'string'],
                                'href'  => ['type' => 'string','description'=>'link to a planning officer report on a planning application'],
                                'resolution'  => ['type' => 'string']
                            ],
                            'required' => ['title','href'],
                            'additionalProperties' => false
                        ]
                    ]
                ],
                'required' => ['meeting_date','apps'],
                'additionalProperties' => false
            ]
        ]
    ]];
}


function llmExtractMeeting(OpenAI\Client $client, string $mainHtml, string $baseUrl, string $model = 'gpt-4o-mini'): array {
    // Trim huge pages
    $maxChars = 60000;
    if (mb_strlen($mainHtml, 'UTF-8') > $maxChars) {
        $mainHtml = mb_substr($mainHtml, 0, $maxChars, 'UTF-8');
    }

    $system = [
        'role' => 'system',
        'content' =>
            "You extract meeting date and application report PDFs from UK local authority committee pages (ModernGov).\n".
            "Return ONLY via the provided function call.\n".
            "- Meeting date: copy as shown; prefer long form (e.g., '10 June 2025').\n".
            "- Include only visible individual application reports; exclude agenda, minutes, packs, front sheets, attendance/apologies, supplements.\n".
            "- Do not fabricate links."
    ];
    $user = [
        'role' => 'user',
        'content' => "BASE URL: {$baseUrl}\n\nHTML (from <main>):\n{$mainHtml}"
    ];

    $response = $client->chat()->create([
        'model'       => $model,
        'messages'    => [$system, $user],
        'tools'       => meeting_extractor_tool(),
        'tool_choice' => ['type'=>'function','function'=>['name'=>'extract_meeting']],
    ]);

    // Safely parse tool-call args (array style)
    $choice    = $response['choices'][0]['message'] ?? [];
    $toolCalls = $choice['tool_calls'] ?? [];
    $argsJson  = $toolCalls[0]['function']['arguments'] ?? '{}';
    $args      = json_decode($argsJson, true) ?: ['meeting_date'=>null,'apps'=>[]];

    // Post-clean titles (drop size notes like "PDF 2 MB")
    foreach ($args['apps'] as &$app) {
        $app['title'] = trim(preg_replace('~\s*pdf\s*\d+(\.\d+)?\s*(kb|mb).*~i', '', (string)$app['title']));
    }
    unset($app);

    return $args;
}

$qs     = $argv[1] ?? '';
parse_str(ltrim($qs, '?'), $args);
$limit  = isset($args['limit']) ? (int)$args['limit'] : 0;
$single = $args['url'] ?? null;

// New: detect --ingest anywhere in $argv
$ingest = (PHP_SAPI === 'cli') && in_array('--ingest', $argv, true);

// Allow browser run
if (php_sapi_name() !== 'cli') {
  header('Content-Type: text/tab-separated-values; charset=UTF-8');
  header('Content-Disposition: attachment; filename="committee_reports.tsv"');
  @set_time_limit(600);
}

function fetch(string $url): ?string {
  $ctx = stream_context_create(['http'=>[
    'method'=>'GET',
    'header'=>"User-Agent: wcc-cttee-scraper/1.0\r\nAccept: text/html\r\n",
    'timeout'=>25,
  ]]);
  $h = @file_get_contents($url, false, $ctx);
  return $h === false ? null : $h;
}

function mysqlDateOrNull(?string $human): ?string {
  if (!$human) return null;
  $human = trim($human);
  // Try flexible parse
  try {
    // Common forms: "Tuesday 14th October, 2025", "14 October 2025", "14/10/2025"
    $human = preg_replace('/(\d{1,2})(st|nd|rd|th)/i', '$1', $human); // drop ordinals
    $human = str_replace(',', '', $human);
    // If slashed day/month/year — normalise to Y-m-d
    if (preg_match('~^\d{1,2}/\d{1,2}/\d{2,4}$~', $human)) {
      $dt = DateTime::createFromFormat('!d/m/Y', $human) ?: DateTime::createFromFormat('!d/m/y', $human);
    } else {
      $dt = new DateTime($human);
    }
    return $dt ? $dt->format('Y-m-d') : null;
  } catch (\Throwable $e) {
    return null;
  }
}

function runGeneralIngester(string $pdfPath, array $meta, string $phpBinary = 'php'): array {
  $metaJson = json_encode($meta, JSON_UNESCAPED_SLASHES);
  $cmd = sprintf(
    '%s %s --file=%s --meta=%s',
    escapeshellcmd($phpBinary),
    escapeshellarg('/home/customer/www/app.westminster-planning.com/private/api/openai/general_ingester.php'),
    escapeshellarg($pdfPath),
    escapeshellarg($metaJson)
  );
  $desc = [1 => ['pipe','w'], 2 => ['pipe','w']];
  $proc = proc_open($cmd, $desc, $pipes, __DIR__);
  if (!is_resource($proc)) return ['ok'=>false, 'error'=>'proc_open failed'];
  $stdout = stream_get_contents($pipes[1]); fclose($pipes[1]);
  $stderr = stream_get_contents($pipes[2]); fclose($pipes[2]);
  $code = proc_close($proc);
  return ['ok' => ($code === 0), 'code' => $code, 'stdout' => $stdout, 'stderr' => $stderr];
}

function xp(string $html): DOMXPath {
  libxml_use_internal_errors(true);
  $dom = new DOMDocument();
  $dom->loadHTML($html);
  libxml_clear_errors();
  return new DOMXPath($dom);
}
function absUrl(string $baseUrl, string $href): string {
  if (preg_match('~^https?://~i', $href)) return $href;
  $p = parse_url($baseUrl);
  $root = $p['scheme'].'://'.$p['host'].(isset($p['port'])?':'.$p['port']:'');
  if ($href !== '' && $href[0] === '/') return $root.$href;
  // resolve relative to current directory (important for "documents/...")
  $dir = preg_replace('~[^/]+$~', '', $p['path'] ?? '/');
  return rtrim($root.$dir, '/').'/'.$href;
}
function t(?DOMNode $n): string { return trim(preg_replace('~\s+~',' ', $n?->textContent ?? '')); }

function firstText(DOMXPath $xp, string $q): string { $n=$xp->query($q)->item(0); return $n? t($n) : ''; }

function cleanAppTitle(string $s): string {
  // drop trailing "PDF 2 MB" etc.
  $s = preg_replace('~\s*pdf\s*\d+\s*(kb|mb).*~i', '', $s);
  return trim($s);
}


function innerText(DOMXPath $xp, array $selectors): string {
  foreach ($selectors as $q) {
    $n = $xp->query($q);
    if ($n && $n->length) return t($n->item(0));
  }
  return '';
}


function extractMeetingDate(DOMXPath $xp, string $html): string {
  // 1) direct <time> tag
  $timeVal = innerText($xp, ["//time/@datetime", "//time[1]"]);
  if ($timeVal) {
    // try normalising e.g. 2025-06-10T18:30:00
    try { $dt = new DateTime($timeVal); return $dt->format('j F Y'); } catch (\Throwable $e) {}
  }

  // 2) common ModernGov containers (varies by instance)
  $candidates = [
    "//*[contains(@class,'mgMeetingDate')]",                      // many sites
    "//*[@id='mgDetailsTable']//*[self::td or self::th][1]",     // details table
    "(//h1)[1]/following::*[self::p or self::div][1]",           // text under H1
    "//*[contains(@class,'mgTitle')]",                           // generic title block
    "(//h1)[1]"                                                  // fallback: H1 itself
  ];
  $blob = '';
  foreach ($candidates as $q) {
    $s = innerText($xp, [$q]);
    if (strlen($s) > 6) { $blob .= ' ' . $s; }
  }
  if (!$blob) $blob = trim(strip_tags($html));

  // 3) robust regexes (ordinal + long month; weekday variants; slashed)
  $months = "(January|February|March|April|May|June|July|August|September|October|November|December)";
  $weekday = "(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)";
  $patterns = [
    "/\\b$weekday,?\\s+\\d{1,2}(st|nd|rd|th)?\\s+$months,?\\s+\\d{4}\\b/i",
    "/\\b\\d{1,2}(st|nd|rd|th)?\\s+$months\\s+\\d{4}\\b/i",
    "/\\b\\d{1,2}\\/(\\d{1,2})\\/(\\d{2,4})\\b/",
  ];
  foreach ($patterns as $rx) {
    if (preg_match($rx, $blob, $m)) return $m[0];
  }
  return '';
}

function fileNameText(string $url): string {
  $name = basename(parse_url($url, PHP_URL_PATH) ?? '');
  // normalise for matching: decode %20, replace separators with spaces
  $name = rawurldecode($name);
  $name = str_replace(['_', '-'], ' ', $name);
  // strip extension
  $name = preg_replace('~\.pdf$~i', '', $name);
  // collapse whitespace
  return trim(preg_replace('~\s+~', ' ', $name));
}

function looksLikeApplicationLabel(string $label): bool {
  $l = strtolower($label);
  // Positive hints
  $pos = [
    'application', 'full planning', 'outline', 'listed building', 'lbc', 'adv', 'advertisement',
    'householder', 'reserved matters', 's73', 'section 73', 'variation', 'rn', 'ref:', 'item '
  ];
  // Negative (boilerplate)
  $neg = ['reports pack', 'frontsheet', 'minutes', 'agenda', 'cover sheet','cover report','coversheet', 'supplement', 'attendance', 'apologies'];
  foreach ($neg as $n) if (str_contains($l, $n)) return false;
  foreach ($pos as $p) if (str_contains($l, $p)) return true;

  // Also accept if anchor text contains a plausible WCC ref pattern
  if (preg_match('~\b\d{2}/\d{5}/(FULL|FUL|OUT|LBC|ADV|HSE|NMA)\b~i', $label)) return true;

  return false;
}

function isHiddenNode(?DOMNode $n): bool {
  for ($node = $n; $node; $node = $node->parentNode) {
    if (!($node instanceof DOMElement)) continue;
    $style = strtolower($node->getAttribute('style'));
    if (strpos($style, 'display:none') !== false) return true;
    if (strpos($style, 'visibility:hidden') !== false) return true;
    $aria = strtolower($node->getAttribute('aria-hidden'));
    if ($aria === 'true') return true;
    $class = strtolower($node->getAttribute('class'));
    if ($class && preg_match('/\b(hidden|sr-only|mgHide|mg-hidden)\b/i', $class)) return true;
  }
  return false;
}

function rowTextForAnchor(DOMElement $a): string {
  // bubble up to the nearest TR
  for ($node = $a; $node; $node = $node->parentNode) {
    if ($node instanceof DOMElement && strtolower($node->tagName) === 'tr') {
      return t($node);
    }
  }
  return t($a);
}

function hasApplicationSignal(string $text): bool {
  $low = strtolower($text);

  // Strong: Westminster-style refs like 24/12345/FULL etc.
  if (preg_match('~\b\d{2}/\d{5}/(full|ful|out|lbc|adv|hse|nma)\b~i', $text)) return true;

  // Other common signals
  $pos = [
    'planning application','listed building','lbc','advertisement','adv',
    'householder','reserved matters','section 73','s73','variation',
    'outline', 'full planning'
  ];
  foreach ($pos as $p) if (str_contains($low, $p)) return true;

  return false;
}

function isNoisyTitle(string $label): bool {
  $s = trim(preg_replace('~\s+~',' ', $label));
  // Explicit noise
  if (preg_match('/^application report\s*-\s*\d+$/i', $s)) return true;
  // Generic one-word "Report" or similar with no signals
  if (preg_match('/^(report|document|attachment)$/i', $s)) return true;
  // Ultra short and no ref-like pattern → suspicious
  if (mb_strlen($s) < 12 && !preg_match('~\d{2}/\d{5}/[A-Z]+~', $s)) return true;
  return false;
}

function encodeUrl(string $url): string {
  $p = parse_url($url);
  if (!$p) return $url;
  $scheme = $p['scheme'] ?? 'https';
  $host   = $p['host'] ?? '';
  $port   = isset($p['port']) ? ':'.$p['port'] : '';
  $path   = implode('/', array_map('rawurlencode', array_map('urldecode', explode('/', $p['path'] ?? '/'))));
  $qs     = isset($p['query']) ? '?'.$p['query'] : '';
  $frag   = isset($p['fragment']) ? '#'.$p['fragment'] : '';
  return "{$scheme}://{$host}{$port}{$path}{$qs}{$frag}";
}

// 1) If single meeting URL provided, just process that
$meetingDocsUrls = [];
if ($single) {
  $meetingDocsUrls = [$single];
} else {
  // Guess correct host from the list page (they sometimes use moderngov host)
  $listUrl = $baseDefault.$listPath;
  $listHtml = fetch($listUrl);
  if (!$listHtml) {
    // Try moderngov host as fallback
    $baseAlt = 'https://camden.moderngov.co.uk';
    $listUrl = $baseAlt.$listPath;
    $listHtml = fetch($listUrl);
    if (!$listHtml) { fwrite(STDERR, "Failed to fetch meetings list on both hosts.\n"); exit(1); }
  }

  $lxp = xp($listHtml);
  $seen = [];
  foreach ($lxp->query("//a[contains(@href,'ieListDocuments.aspx') and contains(@href,'{$ctteeId}')]") as $a) {
    /** @var DOMElement $a */
    $href = absUrl($listUrl, $a->getAttribute('href'));
    $seen[$href] = true;
  }
  $meetingDocsUrls = array_keys($seen);
  sort($meetingDocsUrls);
  if ($limit > 0) $meetingDocsUrls = array_slice($meetingDocsUrls, 0, $limit);
}

//echo "meeting_date\tmeeting_title\tapplication_title\treport_url\tmeeting_docs_url\n";

// 2) For each meeting docs page: pick anchors inside mgItemTable with class mgAiTitleLnk → documents/*.pdf
foreach ($meetingDocsUrls as $docsUrl) {
  $html = fetch($docsUrl);
  if (!$html) continue;
  $xp = xp($html);

  $meetingTitle = firstText($xp, "(//h1)[1]") ?: firstText($xp, "//*[contains(@class,'mgTitle')]");
  $meetingDate  = extractMeetingDate($xp, $html);

  // Prefer anchors inside the ModernGov items table
  $anchors = $xp->query("//table[contains(@class,'mgItemTable')]//a[contains(@href,'documents/')]");
  $foundAny = false;

foreach ($anchors as $a) {
  /** @var DOMElement $a */
  if (isHiddenNode($a)) continue;                          // 1) hidden → skip

  $label = t($a);
  if ($label === '' || isNoisyTitle($label)) continue;     // 2) deny noisy titles early

  $pdf = absUrl($docsUrl, $a->getAttribute('href'));
  if (!preg_match('~\.pdf(\?|$)~i', $pdf)) continue;

  $fnameText = fileNameText($pdf);

  // hard denies you already have
  $l = strtolower($label);
  if (str_contains($l,'reports pack') || str_contains($l,'frontsheet')
      || str_contains($l,'minutes') || str_contains($l,'agenda')
      || str_contains($l,'supplement') || str_contains($l,'attendance')
      || str_contains($l,'apologies')) {
    continue;
  }

  // build row context once
  $rowText = rowTextForAnchor($a);
  $context = $label . ' ' . $rowText;

  // POSITIVE signals (any one is enough):
  $okByLabel    = looksLikeApplicationLabel($label);
  $okByContext  = hasApplicationSignal($context);
  $okByFilename = looksLikeApplicationLabel($fnameText);

  // If none of the positives hit, skip
  if (!($okByLabel || $okByContext || $okByFilename)) continue;

  $title = cleanAppTitle($label);

  $result= [
          'meeting_date'     => $meetingDate,
          'meeting_title'    => $meetingTitle,
          'application_title'=> cleanAppTitle($title),
          'report_url'       => $pdf,
          'meeting_docs_url' => $docsUrl,
        ];
  $foundAny = true;

  echo json_encode(array_values($result), JSON_UNESCAPED_SLASHES | JSON_PRETTY_PRINT);

  if ($ingest) {

    // 2) Map fields to your ingester meta
  $meta = [
    // "application_ref" => null, // not available yet
    "document_type"   => "Committee Report",
    "local_authority" => "Westminster City Council",
    "originator"      => "Westminster City Council",
    "provenance"      => $pdf, // << use the actual PDF URL
    "via"             => "wcc_cttee_scraper",
    "document_date"   => mysqlDateOrNull($meetingDate), // Y-m-d or null
    "meta" => [
      "meeting_docs_url"  => $docsUrl,
      "committee_id"      => $ctteeId,
      "meeting_date_text" => $meetingDate, // keep the human string too
      "application_title" => cleanAppTitle($label ?? ($app['title'] ?? '')),
    ],
  ];

    $res = runGeneralIngester($pdf, $meta);
    if (!$res['ok']) {
      fwrite(STDERR, "[ingest] general_ingester failed (code {$res['code']}): {$res['stderr']}\n");
    } else {
      // optional: log success; you could parse $res['stdout'] if it returns a doc_id
      fwrite(STDERR, "[ingest] OK: {$res['stdout']}\n");
    }

  }



}


}