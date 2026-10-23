import assert from "node:assert/strict";
import { test } from "node:test";
import { listObjects } from "../src/s3.js";

// Minimal ListObjectsV2 over a sorted key list: Prefix, StartAfter, ContinuationToken, MaxKeys
function fakeClient(keys) {
  const sorted = [...keys].sort();
  return {
    calls: 0,
    async send({ input }) {
      this.calls++;
      const after = input.ContinuationToken ?? input.StartAfter ?? "";
      const matches = sorted.filter((k) => k.startsWith(input.Prefix) && k > after);
      const page = matches.slice(0, 3);
      const truncated = matches.length > page.length;
      return {
        Contents: page.map((Key) => ({ Key, LastModified: new Date(0) })),
        IsTruncated: truncated,
        NextContinuationToken: truncated ? page.at(-1) : undefined,
      };
    },
  };
}

test("listObjects shards cover every key exactly once", async () => {
  const ids = ["0abc", "1abc", "1xyz", "9zzz", "Abc", "_odd", "af_afp12345f1", "ma_x", "pdb_00001abc", "z9", "~tilde"];
  const keys = [...ids.map((id) => `structures/${id}_model-1.jpeg`), "other/1abc_model-1.jpeg"];
  for (let i = 0; i < 20; i++) keys.push(`structures/pdb_${String(i).padStart(8, "0")}_model-1.jpeg`);
  const client = fakeClient(keys);
  const seen = [];
  await listObjects(client, "b", "structures/", (o) => seen.push(o.Key));
  assert.deepEqual(seen.sort(), keys.filter((k) => k.startsWith("structures/")).sort());
});
