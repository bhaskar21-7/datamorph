"use strict";

/**
 * pipeline.js — composable, chainable data transformation engine.
 */

class PipelineError extends Error {
  constructor(message, step, transform) {
    super(`[Step ${step} | ${transform}] ${message}`);
    this.name = "PipelineError";
    this.step = step;
    this.transform = transform;
  }
}

class Pipeline {
  /**
   * @param {*} data - Input data (array of objects or single object).
   * @param {object} [options]
   * @param {boolean} [options.strict=true] - Throw on error vs skip step.
   */
  constructor(data, { strict = true } = {}) {
    this._data = JSON.parse(JSON.stringify(data)); // deep clone
    this._strict = strict;
    this._steps = [];
    this._history = [];
    this._warnings = [];
  }

  /**
   * Add a transform step.
   * @param {Function} fn - Transform function.
   * @param {...*} args - Additional arguments to pass to fn.
   */
  apply(fn, ...args) {
    this._steps.push({ name: fn.name || "anonymous", fn, args });
    return this;
  }

  /**
   * Execute the pipeline and return the final data.
   */
  result() {
    let data = this._data;
    let i = 0;
    for (const { name, fn, args } of this._steps) {
      i++;
      const inputSize = Array.isArray(data) ? data.length : 1;
      const start = performance.now();
      try {
        data = fn(data, ...args);
      } catch (e) {
        if (this._strict) {
          throw new PipelineError(e.message, i, name);
        } else {
          this._warnings.push(`[Step ${i} | ${name}] Skipped: ${e.message}`);
          continue;
        }
      }
      const elapsed = performance.now() - start;
      const outputSize = Array.isArray(data) ? data.length : 1;
      this._history.push({ name, durationMs: elapsed, inputSize, outputSize });
    }
    return data;
  }

  /** Per-step execution stats (after .result()). */
  stats() {
    return this._history;
  }

  /** Warnings from skipped steps (if strict=false). */
  warnings() {
    return this._warnings;
  }

  toString() {
    const steps = this._steps.map((s) => s.name).join(" -> ");
    return `Pipeline([${steps}], strict=${this._strict})`;
  }
}

module.exports = { Pipeline, PipelineError };
