#!/usr/bin/env node
/* eslint-disable no-console */

// Convenience wrapper around scraper_idox_validated_range_refs.cjs with a Westminster default start URL.

const path = require("node:path");
const { spawn } = require("node:child_process");
const yargs = require("yargs/yargs");
const { hideBin } = require("yargs/helpers");

const DEFAULT_START_URL =
  "https://idoxpa.westminster.gov.uk/online-applications/search.do?action=advanced&searchType=Application";

const argv = yargs(hideBin(process.argv))
  .scriptName("idox-westminster-validated-range-refs")
  .option("start-url", {
    type: "string",
    describe: "Optional override for Idox advanced search URL",
    default: DEFAULT_START_URL,
  })
  .option("validated-start", {
    type: "string",
    describe: 'Start date for "Date Validated" range. Accepts dd/MM/yyyy or YYYY-MM-DD.',
    demandOption: true,
  })
  .option("validated-end", {
    type: "string",
    describe: 'End date for "Date Validated" range. Accepts dd/MM/yyyy or YYYY-MM-DD.',
    demandOption: true,
  })
  .option("max-pages", {
    type: "number",
    describe: "Max results pages to traverse (safety limit).",
    default: 100,
  })
  .option("ipv4", {
    type: "boolean",
    describe: "Force IPv4 for curl (-4). Helpful if IPv6/DNS is flaky.",
    default: false,
  })
  .option("curl-resolve", {
    type: "array",
    describe:
      'Optional curl --resolve entries (repeatable): e.g. --curl-resolve "idoxpa.westminster.gov.uk:443:1.2.3.4"',
    default: [],
  })
  .option("artifacts", {
    type: "boolean",
    describe: "Write HTML/JSON artifacts to disk (debugging).",
    default: false,
  })
  .option("emit-json", {
    type: "boolean",
    describe: "Emit JSON marker to stdout for downstream parsing.",
    default: true,
  })
  .strict()
  .help()
  .argv;

const baseScript = path.resolve(__dirname, "scraper_idox_validated_range_refs.cjs");

const childArgs = [
  baseScript,
  "--start-url",
  argv["start-url"],
  "--validated-start",
  argv["validated-start"],
  "--validated-end",
  argv["validated-end"],
  "--max-pages",
  String(argv["max-pages"]),
];

if (argv.ipv4) childArgs.push("--ipv4");
if (argv.artifacts) childArgs.push("--artifacts");
if (argv["emit-json"] === false) childArgs.push("--no-emit-json");

for (const entry of argv["curl-resolve"] || []) {
  childArgs.push("--curl-resolve", String(entry));
}

const child = spawn(process.execPath, childArgs, { stdio: "inherit" });
child.on("close", (code) => {
  process.exitCode = code == null ? 1 : code;
});

