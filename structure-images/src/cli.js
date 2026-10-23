#!/usr/bin/env node
import { parseArgs } from "node:util";
import { buildManifest } from "./manifest.js";
import { defaultMolrender, render } from "./render.js";

const USAGE = `Usage: structure-images <command> [options]

Commands:
  manifest   List existing images in S3 and write { "<id>": "<lastModified>" } JSON to a file.
             Run once before the parallel render tasks so they don't each have to ask S3.
    --dest s3://bucket/structures/   S3 prefix the images live under (required)
    --out FILE                       Manifest JSON file to write (required)
    --suffix _model-1.jpeg           Object name suffix that marks an id as rendered

  render     Render images for a list of ids and upload them flat to an S3 prefix.
    --ids FILE                       Newline-separated ids (required)
    --model-url URL                  Base URL of model files; fetches <url>/<id><model-ext> (required)
    --dest s3://bucket/structures/   S3 prefix to upload images to (required)
    --holdings PATH|URL              Holdings file for timestamp checks; repeatable. Without it every id is rendered.
    --manifest FILE                  Output of "manifest"; skips S3 lookups for ids it shows as current
    --model-ext .bcif.gz             Model file extension
    --suffix _model-1.jpeg           Image that must exist for an id to count as rendered
    --concurrency 8                  Renders to run at once
    --check-concurrency 32           S3 HEAD requests to run at once during the timestamp check
    --render all                     molrender mode
    --width 500 --height 500 --format jpeg
    --screen 1280x1024x24            Xvfb screen
    --no-xvfb                        Don't start Xvfb (it is also skipped when DISPLAY is set)
    --molrender PATH                 molrender.js to run
    --tmp-dir /tmp

Common options:
  --endpoint URL   S3 endpoint; defaults to AWS_ENDPOINT_URL_S3 or AWS_ENDPOINT_URL.
                   Credentials come from AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.
`;

const OPTIONS = {
  dest: { type: "string" },
  endpoint: { type: "string" },
  suffix: { type: "string", default: "_model-1.jpeg" },
  out: { type: "string" },
  ids: { type: "string" },
  "model-url": { type: "string" },
  "model-ext": { type: "string", default: ".bcif.gz" },
  holdings: { type: "string", multiple: true, default: [] },
  manifest: { type: "string" },
  concurrency: { type: "string", default: "8" },
  "check-concurrency": { type: "string", default: "32" },
  render: { type: "string", default: "all" },
  width: { type: "string", default: "500" },
  height: { type: "string", default: "500" },
  format: { type: "string", default: "jpeg" },
  screen: { type: "string", default: "1280x1024x24" },
  "no-xvfb": { type: "boolean", default: false },
  molrender: { type: "string" },
  "tmp-dir": { type: "string", default: "/tmp" },
  help: { type: "boolean", short: "h", default: false },
};

function required(values, names) {
  const missing = names.filter((n) => !values[n]);
  if (missing.length) throw new Error(`Missing required option(s): ${missing.map((n) => "--" + n).join(", ")}`);
}

async function main(argv) {
  const { values: v, positionals } = parseArgs({ args: argv, options: OPTIONS, allowPositionals: true });
  const [command] = positionals;
  if (v.help || !command) {
    console.log(USAGE);
    return;
  }
  if (command === "manifest") {
    required(v, ["dest", "out"]);
    await buildManifest({ dest: v.dest, suffix: v.suffix, out: v.out, endpoint: v.endpoint });
  } else if (command === "render") {
    required(v, ["ids", "model-url", "dest"]);
    await render({
      ids: v.ids,
      modelUrl: v["model-url"].replace(/\/+$/, ""),
      modelExt: v["model-ext"],
      dest: v.dest,
      endpoint: v.endpoint,
      holdings: v.holdings,
      manifest: v.manifest,
      suffix: v.suffix,
      concurrency: Number(v.concurrency),
      checkConcurrency: Number(v["check-concurrency"]),
      render: v.render,
      width: v.width,
      height: v.height,
      format: v.format,
      screen: v.screen,
      xvfb: !v["no-xvfb"],
      molrender: v.molrender ?? defaultMolrender(),
      tmpDir: v["tmp-dir"],
    });
  } else {
    throw new Error(`Unknown command ${command}\n\n${USAGE}`);
  }
}

main(process.argv.slice(2)).catch((err) => {
  console.error(err.message);
  process.exitCode = 1;
});
