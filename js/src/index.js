/**
 * datamorph JS — Node.js CLI and library for data transformation
 * Mirrors the Python API surface for JSON, CSV, YAML, XML, NDJSON
 */

const transforms = require("./transforms");
const converters = require("./converters");
const validators = require("./validators");
const { Pipeline } = require("./pipeline");

module.exports = {
  // Pipeline
  Pipeline,

  // Transforms
  ...transforms,

  // Converters
  ...converters,

  // Validators
  ...validators,
};
