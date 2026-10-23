import { readFile } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { gunzipSync } from "node:zlib";

const isUrl = (src) => /^https?:\/\//.test(src);

/** Read a JSON file from a local path or http(s) URL, gunzipping it if needed. */
export async function readJson(src) {
  let buf;
  if (isUrl(src)) {
    const res = await fetch(src, { signal: AbortSignal.timeout(300_000) });
    if (!res.ok) throw new Error(`GET ${src} failed: ${res.status} ${res.statusText}`);
    buf = Buffer.from(await res.arrayBuffer());
  } else {
    buf = await readFile(src);
  }
  if (buf[0] === 0x1f && buf[1] === 0x8b) buf = gunzipSync(buf);
  return JSON.parse(buf.toString("utf8"));
}

function sibling(src, name) {
  return isUrl(src) ? new URL(name, src).href : join(dirname(src), name);
}

function addEntries(out, data, src) {
  for (const [id, value] of Object.entries(data)) {
    // pdb / ihm: { "1ABC": "2024-01-03T00:00:00+0000" }; csm: { "AF_AFP12345F1": { "lastModifiedDate": "...", ... } }
    const stamp = typeof value === "string" ? value : value?.lastModifiedDate;
    const ms = Date.parse(stamp);
    if (Number.isNaN(ms)) throw new Error(`Bad timestamp for ${id} in ${src}: ${JSON.stringify(value)}`);
    out.set(id.toLowerCase(), ms);
  }
}

/**
 * Load holdings files into a Map of lowercase id -> source last-modified time (epoch ms).
 *
 * Each source is a path or URL to a pdb/ihm/csm holdings file. A CSM holdings *list* file
 * ({ "holdings/computed-models-holdings-1.json.gz": count, ... }) is expanded to the holdings
 * files it names, which are read from the same directory as the list.
 */
export async function loadHoldings(sources) {
  const out = new Map();
  for (const src of sources) {
    const data = await readJson(src);
    const values = Object.values(data);
    if (values.length > 0 && values.every((v) => typeof v === "number")) {
      for (const name of Object.keys(data)) {
        const child = sibling(src, basename(name));
        addEntries(out, await readJson(child), child);
      }
    } else {
      addEntries(out, data, src);
    }
  }
  return out;
}
