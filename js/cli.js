#!/usr/bin/env node
"use strict";

/**
 * datamorph CLI
 *
 * Usage:
 *   datamorph <command> [options]
 *
 * Commands:
 *   convert   Convert between formats (json, csv, yaml, xml, ndjson)
 *   transform Apply a transformation to data
 *   validate  Validate data against a schema file
 *   info      Show stats about a data file
 */

const fs = require("fs");
const path = require("path");
const {
  jsonToCsv, csvToJson, jsonToYaml, yamlToJson, jsonToXml,
  csvToNdjson, ndjsonToCsv,
} = require("./src/converters");
const {
  flatten, unflatten, dropNulls, deduplicate, snakeCaseKeys,
} = require("./src/transforms");
const { validateSchema } = require("./src/validators");

const [, , command, ...argv] = process.argv;

const HELP = `
datamorph — composable data transformation CLI

USAGE
  datamorph <command> [options]

COMMANDS
  convert   --input <file> --to <format> [--output <file>]
  transform --input <file> --op <operation> [--output <file>]
  validate  --input <file> --schema <file>
  info      --input <file>

FORMATS
  json, csv, yaml, xml, ndjson

TRANSFORM OPERATIONS
  flatten       Flatten nested objects (dot notation)
  unflatten     Reconstruct nested objects from dot keys
  drop-nulls    Remove null/undefined fields
  deduplicate   Remove duplicate records
  snake-case    Convert all keys to snake_case

EXAMPLES
  datamorph convert --input data.csv --to json
  datamorph convert --input records.json --to csv --output out.csv
  datamorph transform --input data.json --op flatten
  datamorph validate --input data.json --schema schema.json
  datamorph info --input data.json
`;

function readFile(filePath) {
  if (!fs.existsSync(filePath)) {
    die(`File not found: ${filePath}`);
  }
  return fs.readFileSync(filePath, "utf-8");
}

function parseArgs(args) {
  const result = {};
  for (let i = 0; i < args.length; i++) {
    if (args[i].startsWith("--")) {
      result[args[i].slice(2)] = args[i + 1];
      i++;
    }
  }
  return result;
}

function die(msg) {
  console.error(`\n  ✗ Error: ${msg}\n`);
  process.exit(1);
}

function detectFormat(filePath) {
  const ext = path.extname(filePath).toLowerCase().slice(1);
  const map = { json: "json", csv: "csv", yaml: "yaml", yml: "yaml", xml: "xml", ndjson: "ndjson" };
  return map[ext] || null;
}

function parseData(text, format) {
  switch (format) {
    case "json":
      return JSON.parse(text);
    case "csv":
      return csvToJson(text);
    case "yaml":
      return yamlToJson(text);
    case "ndjson":
      return text.trim().split("\n").filter(Boolean).map(JSON.parse);
    default:
      die(`Cannot parse format: ${format}`);
  }
}

function serializeData(data, format) {
  switch (format) {
    case "json":
      return JSON.stringify(data, null, 2);
    case "csv":
      return jsonToCsv(Array.isArray(data) ? data : [data]);
    case "yaml":
      return jsonToYaml(data);
    case "xml":
      return jsonToXml(data);
    case "ndjson":
      return (Array.isArray(data) ? data : [data]).map((r) => JSON.stringify(r)).join("\n");
    default:
      die(`Cannot serialize format: ${format}`);
  }
}

function output(text, outFile) {
  if (outFile) {
    fs.writeFileSync(outFile, text, "utf-8");
    console.log(`  ✓ Written to ${outFile}`);
  } else {
    console.log(text);
  }
}

// ── Commands ──────────────────────────────────────────────────────────────────

function cmdConvert(args) {
  const { input, to, output: outFile } = parseArgs(args);
  if (!input || !to) die("convert requires --input <file> --to <format>");

  const text = readFile(input);
  const fromFormat = detectFormat(input);
  if (!fromFormat) die(`Could not detect format from file: ${input}`);

  const data = parseData(text, fromFormat);
  const result = serializeData(data, to);
  output(result, outFile);
}

function cmdTransform(args) {
  const { input, op, output: outFile } = parseArgs(args);
  if (!input || !op) die("transform requires --input <file> --op <operation>");

  const text = readFile(input);
  const format = detectFormat(input);
  if (!format) die(`Could not detect format from file: ${input}`);

  let data = parseData(text, format);
  switch (op) {
    case "flatten":
      data = flatten(data);
      break;
    case "unflatten":
      data = unflatten(data);
      break;
    case "drop-nulls":
      data = dropNulls(data);
      break;
    case "deduplicate":
      data = deduplicate(data);
      break;
    case "snake-case":
      data = snakeCaseKeys(data);
      break;
    default:
      die(`Unknown operation: ${op}. Available: flatten, unflatten, drop-nulls, deduplicate, snake-case`);
  }

  output(serializeData(data, format), outFile);
}

function cmdValidate(args) {
  const { input, schema: schemaFile } = parseArgs(args);
  if (!input || !schemaFile) die("validate requires --input <file> --schema <file>");

  const text = readFile(input);
  const format = detectFormat(input);
  const data = parseData(text, format);
  const schema = JSON.parse(readFile(schemaFile));

  const result = validateSchema(data, schema);
  if (result.valid) {
    console.log("\n  ✓ Validation passed — all checks ok\n");
  } else {
    console.error(`\n  ✗ Validation failed (${result.errors.length} error(s)):`);
    console.error(result.summary());
    console.error();
    process.exit(1);
  }
}

function cmdInfo(args) {
  const { input } = parseArgs(args);
  if (!input) die("info requires --input <file>");

  const text = readFile(input);
  const format = detectFormat(input);
  const data = parseData(text, format);
  const records = Array.isArray(data) ? data : [data];
  const keys = records.length > 0 ? Object.keys(records[0]) : [];

  const nullCounts = {};
  keys.forEach((k) => {
    nullCounts[k] = records.filter((r) => r[k] === null || r[k] === undefined).length;
  });

  console.log(`
  ─────────────────────────────────────
  datamorph info: ${input}
  ─────────────────────────────────────
  Format   : ${format}
  Records  : ${records.length}
  Fields   : ${keys.length}
  Keys     : ${keys.join(", ")}

  Null counts per field:`);
  keys.forEach((k) => {
    const count = nullCounts[k];
    if (count > 0) console.log(`    ${k}: ${count} null(s)`);
  });
  console.log("  ─────────────────────────────────────\n");
}

// ── Router ────────────────────────────────────────────────────────────────────

switch (command) {
  case "convert":
    cmdConvert(argv);
    break;
  case "transform":
    cmdTransform(argv);
    break;
  case "validate":
    cmdValidate(argv);
    break;
  case "info":
    cmdInfo(argv);
    break;
  case "--help":
  case "-h":
  case undefined:
    console.log(HELP);
    break;
  default:
    console.error(`\n  Unknown command: ${command}`);
    console.log(HELP);
    process.exit(1);
}
