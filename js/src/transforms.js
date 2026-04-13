"use strict";

/**
 * transforms.js — pure, composable data transformation functions.
 * All functions accept a record (object) or array of records.
 */

const isArray = (d) => Array.isArray(d);
const wrap = (d) => (isArray(d) ? d : [d]);
const unwrap = (d, wasList) => (wasList ? d : d[0]);

/**
 * Flatten nested objects into dot-separated keys.
 * @param {Object|Object[]} data
 * @param {string} [separator="."]
 */
function flatten(data, separator = ".") {
  const wasList = isArray(data);
  const records = wrap(data);

  function _flatten(obj, prefix = "") {
    const result = {};
    for (const [k, v] of Object.entries(obj)) {
      const key = prefix ? `${prefix}${separator}${k}` : k;
      if (v !== null && typeof v === "object" && !Array.isArray(v)) {
        Object.assign(result, _flatten(v, key));
      } else {
        result[key] = v;
      }
    }
    return result;
  }

  return unwrap(records.map((r) => _flatten(r)), wasList);
}

/**
 * Reverse of flatten — reconstruct nested objects from dot-separated keys.
 * @param {Object|Object[]} data
 * @param {string} [separator="."]
 */
function unflatten(data, separator = ".") {
  const wasList = isArray(data);
  const records = wrap(data);

  function _unflatten(obj) {
    const result = {};
    for (const [key, value] of Object.entries(obj)) {
      const parts = key.split(separator);
      let d = result;
      for (let i = 0; i < parts.length - 1; i++) {
        d[parts[i]] = d[parts[i]] || {};
        d = d[parts[i]];
      }
      d[parts[parts.length - 1]] = value;
    }
    return result;
  }

  return unwrap(records.map(_unflatten), wasList);
}

/**
 * Rename keys according to a mapping.
 * @param {Object|Object[]} data
 * @param {Record<string, string>} mapping
 */
function renameKeys(data, mapping) {
  const wasList = isArray(data);
  const records = wrap(data);
  const out = records.map((r) =>
    Object.fromEntries(
      Object.entries(r).map(([k, v]) => [mapping[k] ?? k, v])
    )
  );
  return unwrap(out, wasList);
}

/**
 * Keep or exclude specific keys.
 * @param {Object|Object[]} data
 * @param {string[]} keys
 * @param {boolean} [exclude=false]
 */
function filterKeys(data, keys, exclude = false) {
  const wasList = isArray(data);
  const records = wrap(data);
  const keySet = new Set(keys);
  const out = records.map((r) =>
    Object.fromEntries(
      Object.entries(r).filter(([k]) => exclude ? !keySet.has(k) : keySet.has(k))
    )
  );
  return unwrap(out, wasList);
}

/**
 * Apply a function to values of specific keys.
 * @param {Object|Object[]} data
 * @param {Record<string, Function>} mapping
 */
function mapValues(data, mapping) {
  const wasList = isArray(data);
  const records = wrap(data);
  const out = records.map((r) => {
    const result = { ...r };
    for (const [k, fn] of Object.entries(mapping)) {
      if (k in result) result[k] = fn(result[k]);
    }
    return result;
  });
  return unwrap(out, wasList);
}

/**
 * Cast field values to specified types.
 * @param {Object|Object[]} data
 * @param {Record<string, 'string'|'number'|'boolean'|'int'|'float'>} schema
 */
function castTypes(data, schema) {
  const wasList = isArray(data);
  const records = wrap(data);

  function cast(value, type) {
    if (value === null || value === undefined) return value;
    switch (type) {
      case "int":
        return parseInt(value, 10);
      case "float":
      case "number":
        return parseFloat(value);
      case "boolean":
      case "bool": {
        if (typeof value === "string")
          return ["true", "1", "yes"].includes(value.toLowerCase());
        return Boolean(value);
      }
      case "string":
        return String(value);
      default:
        return value;
    }
  }

  const out = records.map((r) => {
    const result = { ...r };
    for (const [k, type] of Object.entries(schema)) {
      if (k in result) result[k] = cast(result[k], type);
    }
    return result;
  });
  return unwrap(out, wasList);
}

/**
 * Remove keys with null/undefined values.
 * @param {Object|Object[]} data
 * @param {string[]} [keys] - If provided, only drop nulls for these keys.
 */
function dropNulls(data, keys = null) {
  const wasList = isArray(data);
  const records = wrap(data);
  const keySet = keys ? new Set(keys) : null;
  const out = records.map((r) =>
    Object.fromEntries(
      Object.entries(r).filter(([k, v]) =>
        keySet ? !(keySet.has(k) && (v === null || v === undefined)) : v !== null && v !== undefined
      )
    )
  );
  return unwrap(out, wasList);
}

/**
 * Remove duplicate records.
 * @param {Object[]} data
 * @param {string} [key] - Deduplicate by this key if provided.
 */
function deduplicate(data, key = null) {
  if (!isArray(data)) return data;
  const seen = new Set();
  return data.filter((r) => {
    const fingerprint = key ? r[key] : JSON.stringify(Object.entries(r).sort());
    if (seen.has(fingerprint)) return false;
    seen.add(fingerprint);
    return true;
  });
}

/**
 * Convert all keys to snake_case.
 * @param {Object|Object[]} data
 */
function snakeCaseKeys(data) {
  const wasList = isArray(data);
  const records = wrap(data);
  const toSnake = (s) =>
    s
      .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
      .replace(/([a-z\d])([A-Z])/g, "$1_$2")
      .replace(/-/g, "_")
      .toLowerCase();
  const out = records.map((r) =>
    Object.fromEntries(Object.entries(r).map(([k, v]) => [toSnake(k), v]))
  );
  return unwrap(out, wasList);
}

/**
 * Add a static field to every record.
 * @param {Object|Object[]} data
 * @param {string} key
 * @param {*} value
 */
function addField(data, key, value) {
  const wasList = isArray(data);
  const records = wrap(data);
  return unwrap(records.map((r) => ({ ...r, [key]: value })), wasList);
}

/**
 * Add a computed field derived from each record.
 * @param {Object|Object[]} data
 * @param {string} key
 * @param {Function} fn
 */
function computeField(data, key, fn) {
  const wasList = isArray(data);
  const records = wrap(data);
  return unwrap(records.map((r) => ({ ...r, [key]: fn(r) })), wasList);
}

module.exports = {
  flatten,
  unflatten,
  renameKeys,
  filterKeys,
  mapValues,
  castTypes,
  dropNulls,
  deduplicate,
  snakeCaseKeys,
  addField,
  computeField,
};
