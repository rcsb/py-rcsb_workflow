import { readFile } from "node:fs/promises";
import { extname } from "node:path";
import { HeadObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";

// Shard boundaries for parallel listing. Each shard lists keys in [bound[i], bound[i+1]) after the prefix,
// so together they cover every key no matter what character it starts with.
const SHARD_BOUNDS = ["", ..."123456789abcdefghijklmnopqrstuvwxyz"];

const CONTENT_TYPES = { ".jpeg": "image/jpeg", ".jpg": "image/jpeg", ".png": "image/png", ".json": "application/json" };

/** Split "s3://bucket/some/prefix/" into { bucket, prefix }. The prefix always ends in "/" (or is empty). */
export function parseS3Url(url) {
  const m = /^s3:\/\/([^/]+)\/?(.*)$/.exec(url);
  if (!m) throw new Error(`Not an s3:// URL: ${url}`);
  let prefix = m[2];
  if (prefix && !prefix.endsWith("/")) prefix += "/";
  return { bucket: m[1], prefix };
}

/**
 * S3 client for SeaweedFS. Credentials come from AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY.
 * The endpoint comes from --endpoint, AWS_ENDPOINT_URL_S3 or AWS_ENDPOINT_URL.
 */
export function makeClient(endpoint) {
  return new S3Client({
    endpoint: endpoint || process.env.AWS_ENDPOINT_URL_S3 || process.env.AWS_ENDPOINT_URL,
    region: process.env.AWS_REGION || "us-east-1",
    forcePathStyle: true,
    // Newer SDKs add CRC checksums to every request by default, which S3-compatible stores don't always accept.
    requestChecksumCalculation: "WHEN_REQUIRED",
    responseChecksumValidation: "WHEN_REQUIRED",
  });
}

async function listRange(client, bucket, prefix, lo, hi, onObject) {
  const stopAt = hi ? prefix + hi : null;
  let token;
  do {
    const res = await client.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix,
        StartAfter: token ? undefined : lo ? prefix + lo : undefined,
        ContinuationToken: token,
        MaxKeys: 1000,
      }),
    );
    for (const obj of res.Contents ?? []) {
      if (stopAt && obj.Key >= stopAt) return;
      onObject(obj);
    }
    token = res.IsTruncated ? res.NextContinuationToken : undefined;
  } while (token);
}

/** List every object under prefix, running the shards in parallel. Calls onObject({ Key, LastModified, ... }). */
export async function listObjects(client, bucket, prefix, onObject) {
  await Promise.all(SHARD_BOUNDS.map((lo, i) => listRange(client, bucket, prefix, lo, SHARD_BOUNDS[i + 1], onObject)));
}

/** Last-modified time of an object in epoch ms, or null if it doesn't exist. */
export async function headMtime(client, bucket, key) {
  try {
    const res = await client.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
    return res.LastModified.getTime();
  } catch (err) {
    if (err.name === "NotFound" || err.$metadata?.httpStatusCode === 404) return null;
    throw err;
  }
}

export async function uploadFile(client, bucket, key, filePath) {
  await client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: await readFile(filePath),
      ContentType: CONTENT_TYPES[extname(filePath).toLowerCase()] ?? "application/octet-stream",
    }),
  );
}
