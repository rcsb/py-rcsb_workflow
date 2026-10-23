import { spawn } from "node:child_process";
import { mkdir, mkdtemp, readdir, rm, stat, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { join } from "node:path";
import { loadHoldings } from "./holdings.js";
import { readManifest } from "./manifest.js";
import { headMtime, makeClient, parseS3Url, uploadFile } from "./s3.js";
import { log, mapLimit, readIdList } from "./util.js";

export function defaultMolrender() {
  return createRequire(import.meta.url).resolve("molrender/build/bin/molrender.js");
}

/**
 * Decide which ids need rendering. Without holdings every id is rendered. With holdings an id is
 * skipped when its image exists and is at least as new as the source (or the id isn't in holdings).
 *
 * The manifest, when given, answers "up to date" without a request. Anything it doesn't vouch for is
 * confirmed with head(id), so a retry with a stale manifest doesn't redo ids that have since been uploaded.
 */
export async function selectIds({ ids, holdings, manifest, head, concurrency }) {
  if (!holdings) return ids;
  const isCurrent = (id, mtime) => mtime != null && (!holdings.has(id) || mtime >= holdings.get(id));
  const flags = await mapLimit(ids, concurrency, async (id) => {
    if (manifest && isCurrent(id, manifest.get(id))) return false;
    return !isCurrent(id, await head(id));
  });
  return ids.filter((_, i) => flags[i]);
}

/** Start one Xvfb for every render to share. Xvfb picks a free display and reports it on fd 3. */
function startXvfb(screen) {
  return new Promise((resolve, reject) => {
    const proc = spawn("Xvfb", ["-displayfd", "3", "-screen", "0", screen, "-ac", "-nolisten", "tcp"], {
      stdio: ["ignore", "ignore", "ignore", "pipe"],
    });
    let buf = "";
    proc.stdio[3].on("data", (d) => {
      buf += d;
      if (buf.includes("\n")) resolve({ display: `:${buf.trim()}`, stop: () => proc.kill() });
    });
    proc.on("error", reject);
    proc.on("exit", (code) => reject(new Error(`Xvfb exited with code ${code}`)));
  });
}

function run(cmd, args, env) {
  return new Promise((resolve, reject) => {
    const proc = spawn(cmd, args, { env, stdio: ["ignore", "pipe", "pipe"] });
    let output = "";
    proc.stdout.on("data", (d) => (output += d));
    proc.stderr.on("data", (d) => (output += d));
    proc.on("error", reject);
    proc.on("close", (code, signal) => {
      if (code === 0) resolve(output);
      else reject(new Error(`${cmd} ${args.join(" ")} exited with ${signal ?? code}\n${output.slice(-4000)}`));
    });
  });
}

async function download(url, path, attempts = 3) {
  for (let i = 1; ; i++) {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(120_000) });
      if (!res.ok) throw new Error(`GET ${url} failed: ${res.status} ${res.statusText}`);
      await writeFile(path, Buffer.from(await res.arrayBuffer()));
      return;
    } catch (err) {
      // A 404 won't fix itself, so don't retry it.
      if (i >= attempts || /failed: 404/.test(err.message)) throw err;
      await new Promise((r) => setTimeout(r, 2000 * i));
    }
  }
}

/** Download one model, render it, and hand each image to upload(name, path). Returns the number of images. */
export async function renderOne(id, o) {
  const work = await mkdtemp(join(o.tmpDir, `${id}-`));
  try {
    const model = join(work, id + o.modelExt);
    await download(`${o.modelUrl}/${id}${o.modelExt}`, model);
    const outDir = join(work, "out");
    await mkdir(outDir);
    const output = await run(
      process.execPath,
      [o.molrender, o.render, model, outDir, "--height", String(o.height), "--width", String(o.width), "--format", o.format],
      o.env,
    );
    const checkFile = id + o.suffix;
    const checkStat = await stat(join(outDir, checkFile)).catch(() => null);
    if (!checkStat?.size) throw new Error(`molrender did not produce ${checkFile}\n${output.slice(-4000)}`);
    // Upload the check file last so a partly uploaded id still looks missing and gets redone.
    const files = (await readdir(outDir)).sort((a, b) => (a === checkFile) - (b === checkFile));
    for (const name of files) await o.upload(name, join(outDir, name));
    return files.length;
  } finally {
    await rm(work, { recursive: true, force: true });
  }
}

export async function render(opts) {
  const ids = await readIdList(opts.ids);
  log(`Read ${ids.length} ids from ${opts.ids}`);
  const client = makeClient(opts.endpoint);
  const { bucket, prefix } = parseS3Url(opts.dest);

  const holdings = opts.holdings.length ? await loadHoldings(opts.holdings) : null;
  if (holdings) log(`Loaded ${holdings.size} holdings timestamps`);
  const manifest = opts.manifest ? await readManifest(opts.manifest) : null;
  if (manifest) log(`Loaded ${manifest.size} existing image timestamps from ${opts.manifest}`);
  const todo = await selectIds({
    ids,
    holdings,
    manifest,
    head: (id) => headMtime(client, bucket, prefix + id + opts.suffix),
    concurrency: opts.checkConcurrency,
  });
  log(`${todo.length} of ${ids.length} ids need images`);
  if (todo.length === 0) return;

  let xvfb = null;
  const env = { ...process.env };
  if (opts.xvfb && !env.DISPLAY) {
    xvfb = await startXvfb(opts.screen);
    env.DISPLAY = xvfb.display;
    log(`Started Xvfb on ${xvfb.display}`);
  }

  const o = {
    ...opts,
    env,
    upload: (name, path) => uploadFile(client, bucket, prefix + name, path),
  };
  const failed = [];
  let done = 0;
  try {
    await mapLimit(todo, opts.concurrency, async (id) => {
      const start = Date.now();
      try {
        const n = await renderOne(id, o);
        log(`[${++done}/${todo.length}] ${id}: uploaded ${n} images in ${((Date.now() - start) / 1000).toFixed(1)}s`);
      } catch (err) {
        done++;
        failed.push(id);
        log(`[${done}/${todo.length}] ${id}: FAILED: ${err.message}`);
      }
    });
  } finally {
    xvfb?.stop();
  }

  if (failed.length) throw new Error(`Image generation failed for ${failed.length} ids: ${failed.join(" ")}`);
  log(`Done: ${todo.length} ids uploaded to s3://${bucket}/${prefix}`);
}
