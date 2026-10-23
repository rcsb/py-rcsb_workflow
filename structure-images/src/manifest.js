import { readFile, writeFile } from "node:fs/promises";
import { listObjects, makeClient, parseS3Url } from "./s3.js";
import { log } from "./util.js";

/**
 * List every "<id><suffix>" object under an S3 prefix and write { "<id>": "<lastModified ISO time>" } JSON to outPath.
 * One run of this replaces a HEAD request per id in every render task.
 */
export async function buildManifest({ dest, suffix, out, endpoint }) {
  const client = makeClient(endpoint);
  const { bucket, prefix } = parseS3Url(dest);
  const entries = [];
  let seen = 0;
  const start = Date.now();
  await listObjects(client, bucket, prefix, (obj) => {
    seen++;
    if (obj.Key.endsWith(suffix)) entries.push([obj.Key.slice(prefix.length, -suffix.length), obj.LastModified.toISOString()]);
  });
  entries.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  await writeFile(out, JSON.stringify(Object.fromEntries(entries)) + "\n");
  log(`Listed ${seen} objects under s3://${bucket}/${prefix} in ${((Date.now() - start) / 1000).toFixed(1)}s; wrote ${entries.length} ids to ${out}`);
}

/** Read a manifest written by buildManifest into a Map of id -> lastModified (epoch ms). */
export async function readManifest(path) {
  const out = new Map();
  for (const [id, stamp] of Object.entries(JSON.parse(await readFile(path, "utf8")))) {
    const ms = Date.parse(stamp);
    if (Number.isNaN(ms)) throw new Error(`Bad timestamp for ${id} in ${path}: ${JSON.stringify(stamp)}`);
    out.set(id.toLowerCase(), ms);
  }
  return out;
}
