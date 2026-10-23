import { readFile } from "node:fs/promises";

export function log(...args) {
  console.log(new Date().toISOString(), ...args);
}

/** Run fn over items with at most `limit` calls in flight. Results keep input order. */
export async function mapLimit(items, limit, fn) {
  const results = new Array(items.length);
  let next = 0;
  async function worker() {
    while (next < items.length) {
      const i = next++;
      results[i] = await fn(items[i], i);
    }
  }
  await Promise.all(Array.from({ length: Math.min(limit, items.length) }, worker));
  return results;
}

/** Read a newline-separated id list, lowercased, blank lines dropped. */
export async function readIdList(path) {
  const text = await readFile(path, "utf8");
  return text
    .split(/\r?\n/)
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}
