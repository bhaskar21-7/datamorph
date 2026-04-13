"use strict";

/**
 * converters.js — bidirectional format conversion utilities.
 * JSON, CSV, YAML, XML, NDJSON
 */

/**
 * Convert array of objects to CSV string.
 * @param {Object[]} data
 * @param {string[]} [fieldnames]
 * @param {string} [delimiter=","]
 */
function jsonToCsv(data, fieldnames = null, delimiter = ",") {
  if (!Array.isArray(data)) data = [data];
  if (data.length === 0) return "";
  const keys = fieldnames || Object.keys(data[0]);
  const escape = (v) => {
    const s = v === null || v === undefined ? "" : String(v);
    return s.includes(delimiter) || s.includes('"') || s.includes("\n")
      ? `"${s.replace(/"/g, '""')}"`
      : s;
  };
  const header = keys.map(escape).join(delimiter);
  const rows = data.map((r) => keys.map((k) => escape(r[k] ?? "")).join(delimiter));
  return [header, ...rows].join("\n");
}

/**
 * Parse CSV string into array of objects.
 * @param {string} text
 * @param {string} [delimiter=","]
 * @param {boolean} [inferTypes=true]
 */
function csvToJson(text, delimiter = ",", inferTypes = true) {
  const lines = text.trim().split("\n");
  if (lines.length < 2) return [];
  const headers = parseCsvLine(lines[0], delimiter);
  const records = [];
  for (let i = 1; i < lines.length; i++) {
    const values = parseCsvLine(lines[i], delimiter);
    const record = {};
    headers.forEach((h, j) => {
      record[h] = inferTypes ? inferType(values[j] ?? "") : (values[j] ?? "");
    });
    records.push(record);
  }
  return records;
}

function parseCsvLine(line, delimiter) {
  const result = [];
  let current = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i++;
      } else {
        inQuotes = !inQuotes;
      }
    } else if (ch === delimiter && !inQuotes) {
      result.push(current);
      current = "";
    } else {
      current += ch;
    }
  }
  result.push(current);
  return result;
}

function inferType(value) {
  if (value === "") return null;
  if (value.toLowerCase() === "true") return true;
  if (value.toLowerCase() === "false") return false;
  const num = Number(value);
  if (!isNaN(num) && value.trim() !== "") return num;
  return value;
}

/**
 * Convert data to YAML string. Requires 'js-yaml'.
 * @param {*} data
 */
function jsonToYaml(data) {
  try {
    const yaml = require("js-yaml");
    return yaml.dump(data);
  } catch {
    throw new Error("js-yaml is required: npm install js-yaml");
  }
}

/**
 * Parse YAML string to JS object. Requires 'js-yaml'.
 * @param {string} text
 */
function yamlToJson(text) {
  try {
    const yaml = require("js-yaml");
    return yaml.load(text);
  } catch {
    throw new Error("js-yaml is required: npm install js-yaml");
  }
}

/**
 * Convert a JS object/array to XML string.
 * @param {*} data
 * @param {string} [root="root"]
 * @param {string} [itemTag="item"]
 * @param {number} [indent=2]
 */
function jsonToXml(data, root = "root", itemTag = "item", indent = 2) {
  const pad = " ".repeat(indent);

  function escape(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function toXml(obj, tag, level = 0) {
    const prefix = pad.repeat(level);
    if (Array.isArray(obj)) {
      const children = obj.map((item) => toXml(item, itemTag, level + 1)).join("\n");
      return `${prefix}<${tag}>\n${children}\n${prefix}</${tag}>`;
    }
    if (obj !== null && typeof obj === "object") {
      const children = Object.entries(obj)
        .map(([k, v]) => toXml(v, k, level + 1))
        .join("\n");
      return `${prefix}<${tag}>\n${children}\n${prefix}</${tag}>`;
    }
    return `${prefix}<${tag}>${escape(obj)}</${tag}>`;
  }

  return `<?xml version="1.0" encoding="UTF-8"?>\n${toXml(data, root)}`;
}

/**
 * Convert CSV to NDJSON (newline-delimited JSON).
 * @param {string} text
 * @param {string} [delimiter=","]
 */
function csvToNdjson(text, delimiter = ",") {
  const records = csvToJson(text, delimiter);
  return records.map((r) => JSON.stringify(r)).join("\n");
}

/**
 * Convert NDJSON to CSV string.
 * @param {string} text
 */
function ndjsonToCsv(text) {
  const records = text
    .trim()
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line));
  return jsonToCsv(records);
}

module.exports = {
  jsonToCsv,
  csvToJson,
  jsonToYaml,
  yamlToJson,
  jsonToXml,
  csvToNdjson,
  ndjsonToCsv,
};
