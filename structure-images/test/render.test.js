import assert from "node:assert/strict";
import { createServer } from "node:http";
import { chmod, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { gzipSync } from "node:zlib";
import { test } from "node:test";
import { loadHoldings } from "../src/holdings.js";
import { readManifest } from "../src/manifest.js";
import { renderOne, selectIds } from "../src/render.js";
import { parseS3Url } from "../src/s3.js";
import { mapLimit } from "../src/util.js";

const T0 = Date.parse("2024-01-01T00:00:00+0000");
const T1 = Date.parse("2024-06-01T00:00:00+0000");

test("parseS3Url", () => {
  assert.deepEqual(parseS3Url("s3://images-a/structures/"), { bucket: "images-a", prefix: "structures/" });
  assert.deepEqual(parseS3Url("s3://images-a/structures"), { bucket: "images-a", prefix: "structures/" });
  assert.deepEqual(parseS3Url("s3://images-a"), { bucket: "images-a", prefix: "" });
  assert.throws(() => parseS3Url("/local/path"));
});

test("mapLimit keeps order and caps concurrency", async () => {
  let inFlight = 0;
  let peak = 0;
  const out = await mapLimit([1, 2, 3, 4, 5, 6], 2, async (x) => {
    peak = Math.max(peak, ++inFlight);
    await new Promise((r) => setTimeout(r, 5));
    inFlight--;
    return x * 10;
  });
  assert.deepEqual(out, [10, 20, 30, 40, 50, 60]);
  assert.equal(peak, 2);
});

test("loadHoldings reads pdb and gzipped csm list formats", async () => {
  const dir = await mkdtemp(join(tmpdir(), "holdings-"));
  const pdb = join(dir, "released_structures_last_modified_dates.json.gz");
  await writeFile(pdb, gzipSync(JSON.stringify({ "1ABC": "2024-01-01T00:00:00+0000" })));
  await writeFile(join(dir, "computed-models-holdings-1.json.gz"), gzipSync(JSON.stringify({ AF_X1: { lastModifiedDate: "2024-06-01T00:00:00+0000" } })));
  const list = join(dir, "computed-models-holdings-list.json");
  await writeFile(list, JSON.stringify({ "holdings/computed-models-holdings-1.json.gz": 1 }));

  const h = await loadHoldings([pdb, list]);
  assert.deepEqual([...h], [["1abc", T0], ["af_x1", T1]]);
});

test("selectIds", async () => {
  const holdings = new Map([["new", T1], ["stale", T1], ["fresh", T1], ["missing", T1]]);
  const s3 = new Map([["stale", T0], ["fresh", T1], ["unheld", T0]]);
  const heads = [];
  const head = async (id) => (heads.push(id), s3.get(id) ?? null);
  const ids = ["new", "stale", "fresh", "missing", "unheld"];

  // No holdings: render everything, no lookups
  assert.deepEqual(await selectIds({ ids, holdings: null, head, concurrency: 4 }), ids);
  assert.equal(heads.length, 0);

  // HEAD mode: render what's missing or older than the source; ids not in holdings only if missing
  assert.deepEqual(await selectIds({ ids, holdings, head, concurrency: 4 }), ["new", "stale", "missing"]);
  assert.equal(heads.length, 5);

  // Manifest mode: only ids the manifest doesn't vouch for get a HEAD
  heads.length = 0;
  const manifest = new Map([["fresh", T1], ["unheld", T0], ["stale", T0]]);
  s3.set("stale", T1); // uploaded since the manifest was taken (e.g. an Airflow retry)
  assert.deepEqual(await selectIds({ ids, holdings, manifest, head, concurrency: 4 }), ["new", "missing"]);
  assert.deepEqual(heads.sort(), ["missing", "new", "stale"]);
});

test("readManifest", async () => {
  const dir = await mkdtemp(join(tmpdir(), "manifest-"));
  const path = join(dir, "m.json");
  await writeFile(path, JSON.stringify({ "1abc": new Date(T0).toISOString(), PDB_00001ABC: "2024-06-01T00:00:00+0000" }));
  assert.deepEqual([...(await readManifest(path))], [["1abc", T0], ["pdb_00001abc", T1]]);
});

test("renderOne downloads, renders and uploads the check file last", async () => {
  const dir = await mkdtemp(join(tmpdir(), "render-"));
  // Stand-in for molrender.js: args are <mode> <in> <outDir> ...; writes a few images named after the input
  const fake = join(dir, "fake-molrender.js");
  await writeFile(
    fake,
    `const fs = require("fs"), path = require("path");
     const [, , mode, input, out] = process.argv;
     const id = path.basename(input).split(".")[0];
     if (fs.readFileSync(input, "utf8") !== "MODEL " + id) process.exit(3);
     for (const n of ["_model-1", "_assembly-1", "_chain-A"]) fs.writeFileSync(path.join(out, id + n + ".jpeg"), "jpg");`,
  );
  await writeFile(join(dir, "package.json"), '{"type":"commonjs"}');
  await chmod(fake, 0o755);

  const server = createServer((req, res) => {
    const m = /^\/pdb\/(\w+)\.bcif\.gz$/.exec(req.url);
    if (!m) return res.writeHead(404).end();
    res.end(`MODEL ${m[1]}`);
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const base = `http://127.0.0.1:${server.address().port}/pdb`;

  const uploads = [];
  const o = {
    tmpDir: dir,
    modelUrl: base,
    modelExt: ".bcif.gz",
    molrender: fake,
    render: "all",
    height: "500",
    width: "500",
    format: "jpeg",
    suffix: "_model-1.jpeg",
    env: process.env,
    upload: async (name, path) => uploads.push([name, await readFile(path, "utf8")]),
  };
  try {
    assert.equal(await renderOne("1abc", o), 3);
    assert.equal(uploads.length, 3);
    assert.deepEqual(uploads.at(-1), ["1abc_model-1.jpeg", "jpg"]);
    await assert.rejects(renderOne("no-such-id", o), /404/);
  } finally {
    server.close();
  }
});
