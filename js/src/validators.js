"use strict";

/**
 * validators.js — schema and data validation utilities.
 */

class ValidationResult {
  constructor(valid, errors = []) {
    this.valid = valid;
    this.errors = errors;
  }

  toString() {
    return this.valid
      ? "ValidationResult(✓ valid)"
      : `ValidationResult(✗ invalid, ${this.errors.length} error(s))`;
  }

  summary() {
    return this.valid
      ? "All checks passed."
      : this.errors.map((e) => `  - ${e}`).join("\n");
  }
}

function toRecords(data) {
  return Array.isArray(data) ? data : [data];
}

/**
 * Check that all required keys are present and non-null.
 */
function validateRequired(data, keys) {
  const errors = [];
  toRecords(data).forEach((r, i) => {
    keys.forEach((k) => {
      if (!(k in r)) errors.push(`Record ${i}: missing required key '${k}'`);
      else if (r[k] === null || r[k] === undefined)
        errors.push(`Record ${i}: key '${k}' is null`);
    });
  });
  return new ValidationResult(!errors.length, errors);
}

/**
 * Validate types of field values.
 * schema: { key: 'string' | 'number' | 'boolean' | 'object' | 'array' }
 */
function validateTypes(data, schema, allowNull = true) {
  const errors = [];
  toRecords(data).forEach((r, i) => {
    for (const [k, expectedType] of Object.entries(schema)) {
      if (!(k in r)) continue;
      const v = r[k];
      if ((v === null || v === undefined) && allowNull) continue;
      const actual =
        Array.isArray(v) ? "array" : v === null ? "null" : typeof v;
      if (actual !== expectedType)
        errors.push(
          `Record ${i}: key '${k}' expected ${expectedType}, got ${actual} (${JSON.stringify(v)})`
        );
    }
  });
  return new ValidationResult(!errors.length, errors);
}

/**
 * Validate numeric/string values are within allowed ranges.
 * rules: { key: { min, max, minLen, maxLen, choices } }
 */
function validateRange(data, rules) {
  const errors = [];
  toRecords(data).forEach((r, i) => {
    for (const [k, c] of Object.entries(rules)) {
      if (!(k in r) || r[k] === null) continue;
      const v = r[k];
      if (c.min !== undefined && v < c.min)
        errors.push(`Record ${i}: '${k}' = ${v} is below min ${c.min}`);
      if (c.max !== undefined && v > c.max)
        errors.push(`Record ${i}: '${k}' = ${v} exceeds max ${c.max}`);
      if (c.minLen !== undefined && String(v).length < c.minLen)
        errors.push(`Record ${i}: '${k}' length ${String(v).length} below minLen ${c.minLen}`);
      if (c.maxLen !== undefined && String(v).length > c.maxLen)
        errors.push(`Record ${i}: '${k}' length ${String(v).length} exceeds maxLen ${c.maxLen}`);
      if (c.choices && !c.choices.includes(v))
        errors.push(`Record ${i}: '${k}' = ${JSON.stringify(v)} not in choices ${JSON.stringify(c.choices)}`);
    }
  });
  return new ValidationResult(!errors.length, errors);
}

/**
 * Full schema validation.
 * schema: { key: { type, required, min, max, minLen, maxLen, choices } }
 */
function validateSchema(data, schema) {
  const errors = [];
  const requiredKeys = Object.entries(schema)
    .filter(([, v]) => v.required)
    .map(([k]) => k);
  const typeSchema = Object.fromEntries(
    Object.entries(schema).filter(([, v]) => v.type).map(([k, v]) => [k, v.type])
  );
  const rangeRules = Object.fromEntries(
    Object.entries(schema).map(([k, v]) => {
      const { min, max, minLen, maxLen, choices } = v;
      return [k, { min, max, minLen, maxLen, choices }];
    })
  );

  if (requiredKeys.length) errors.push(...validateRequired(data, requiredKeys).errors);
  if (Object.keys(typeSchema).length) errors.push(...validateTypes(data, typeSchema).errors);
  if (Object.values(rangeRules).some((r) => Object.values(r).some((v) => v !== undefined)))
    errors.push(...validateRange(data, rangeRules).errors);

  return new ValidationResult(!errors.length, errors);
}

module.exports = {
  ValidationResult,
  validateRequired,
  validateTypes,
  validateRange,
  validateSchema,
};
