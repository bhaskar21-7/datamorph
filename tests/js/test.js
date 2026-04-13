"use strict";

const assert = require("assert/strict");
const { test, describe } = require("node:test");

const {
  flatten, unflatten, renameKeys, filterKeys,
  castTypes, dropNulls, deduplicate, snakeCaseKeys,
} = require("../../js/src/transforms");

const {
  jsonToCsv, csvToJson, jsonToXml, csvToNdjson, ndjsonToCsv,
} = require("../../js/src/converters");

const {
  validateRequired, validateTypes, validateRange, validateSchema,
} = require("../../js/src/validators");

const { Pipeline, PipelineError } = require("../../js/src/pipeline");

// ── Transforms ───────────────────────────────────────────────────────────────

describe("flatten", () => {
  test("basic nested object", () => {
    const result = flatten({ a: { b: { c: 1 } } });
    assert.deepEqual(result, { "a.b.c": 1 });
  });

  test("list of records", () => {
    const result = flatten([{ x: { y: 1 } }, { x: { y: 2 } }]);
    assert.deepEqual(result, [{ "x.y": 1 }, { "x.y": 2 }]);
  });

  test("custom separator", () => {
    assert.deepEqual(flatten({ a: { b: 1 } }, "/"), { "a/b": 1 });
  });

  test("roundtrip with unflatten", () => {
    const original = { a: { b: { c: 42 } } };
    assert.deepEqual(unflatten(flatten(original)), original);
  });
});

describe("renameKeys", () => {
  test("renames specified keys", () => {
    const result = renameKeys({ firstName: "Jane" }, { firstName: "first_name" });
    assert.deepEqual(result, { first_name: "Jane" });
  });
});

describe("filterKeys", () => {
  test("include mode", () => {
    assert.deepEqual(filterKeys({ a: 1, b: 2, c: 3 }, ["a", "c"]), { a: 1, c: 3 });
  });

  test("exclude mode", () => {
    assert.deepEqual(filterKeys({ a: 1, b: 2 }, ["b"], true), { a: 1 });
  });
});

describe("castTypes", () => {
  test("cast string to int, float, bool", () => {
    const result = castTypes({ age: "25", score: "9.5", active: "true" }, {
      age: "int", score: "float", active: "bool",
    });
    assert.equal(result.age, 25);
    assert.equal(result.score, 9.5);
    assert.equal(result.active, true);
  });
});

describe("dropNulls", () => {
  test("removes null fields", () => {
    assert.deepEqual(dropNulls({ a: 1, b: null, c: undefined }), { a: 1 });
  });
});

describe("deduplicate", () => {
  test("by full record", () => {
    const result = deduplicate([{ a: 1 }, { a: 2 }, { a: 1 }]);
    assert.equal(result.length, 2);
  });

  test("by key", () => {
    const result = deduplicate(
      [{ id: 1, v: "x" }, { id: 1, v: "y" }, { id: 2, v: "z" }],
      "id"
    );
    assert.equal(result.length, 2);
  });
});

describe("snakeCaseKeys", () => {
  test("converts camelCase to snake_case", () => {
    assert.deepEqual(
      snakeCaseKeys({ firstName: "Jane", lastName: "Doe" }),
      { first_name: "Jane", last_name: "Doe" }
    );
  });
});

// ── Converters ───────────────────────────────────────────────────────────────

describe("json <-> csv", () => {
  test("roundtrip", () => {
    const data = [{ name: "Alice", age: 30 }, { name: "Bob", age: 25 }];
    const csv = jsonToCsv(data);
    const result = csvToJson(csv);
    assert.equal(result[0].name, "Alice");
    assert.equal(result[0].age, 30);
  });
});

describe("json -> xml", () => {
  test("contains root tag", () => {
    const xml = jsonToXml({ name: "Alice" }, "person");
    assert.ok(xml.includes("<person>"));
  });
});

describe("csv <-> ndjson", () => {
  test("roundtrip", () => {
    const csv = "name,age\nAlice,30\nBob,25";
    const ndjson = csvToNdjson(csv);
    const result = ndjsonToCsv(ndjson);
    assert.ok(result.includes("Alice"));
  });
});

// ── Validators ───────────────────────────────────────────────────────────────

describe("validateRequired", () => {
  test("passes when all keys present", () => {
    assert.ok(validateRequired([{ a: 1, b: 2 }], ["a", "b"]).valid);
  });

  test("fails on missing key", () => {
    assert.ok(!validateRequired([{ a: 1 }], ["a", "b"]).valid);
  });
});

describe("validateTypes", () => {
  test("passes correct types", () => {
    assert.ok(validateTypes([{ age: 25, name: "Alice" }], { age: "number", name: "string" }).valid);
  });

  test("fails wrong type", () => {
    assert.ok(!validateTypes([{ age: "25" }], { age: "number" }).valid);
  });
});

describe("validateRange", () => {
  test("fails out-of-range number", () => {
    assert.ok(!validateRange([{ score: 150 }], { score: { min: 0, max: 100 } }).valid);
  });

  test("fails invalid choice", () => {
    assert.ok(!validateRange([{ status: "pending" }], { status: { choices: ["active", "inactive"] } }).valid);
  });
});

// ── Pipeline ─────────────────────────────────────────────────────────────────

describe("Pipeline", () => {
  test("chains transforms", () => {
    const data = [{ firstName: "Jane", age: "30", junk: null }];
    const result = new Pipeline(data)
      .apply(snakeCaseKeys)
      .apply(castTypes, { age: "int" })
      .apply(dropNulls)
      .result();
    assert.equal(result[0].first_name, "Jane");
    assert.equal(result[0].age, 30);
    assert.ok(!("junk" in result[0]));
  });

  test("strict mode throws PipelineError", () => {
    const bad = (d) => { throw new Error("boom"); };
    assert.throws(() => new Pipeline([{ a: 1 }]).apply(bad).result(), PipelineError);
  });

  test("non-strict mode skips errored steps", () => {
    const bad = (d) => { throw new Error("boom"); };
    const result = new Pipeline([{ a: 1 }], { strict: false }).apply(bad).result();
    assert.deepEqual(result, [{ a: 1 }]);
  });
});
