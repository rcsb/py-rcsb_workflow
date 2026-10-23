# structure-images

Node replacement for `imgs_exec_cli --op genJpgs`. It renders structure JPEGs with [molrender](https://www.npmjs.com/package/molrender) and uploads them straight to SeaweedFS over S3, flat under one prefix (`s3://images-a/structures/1abc_model-1.jpeg`). It builds its own image, `rcsb/structure-images`, from [Dockerfile](Dockerfile). The Python CLI and `rcsb/rcsb-workflow` image are untouched.

Compared with the Python version:

- Models always come from a URL (`--model-url`); there is no local bcif path.
- Images go to S3, not a hashed directory tree on a PVC.
- The "is this image out of date?" check compares holdings timestamps with S3 `LastModified` instead of file mtimes.
- One Xvfb server is shared by all renders in a pod, instead of an `xvfb-run` per id.

## Commands

```bash
# Once per run: list what's already in S3 (parallel ListObjectsV2, ~1000 keys per request).
# Writes {"1abc": "2024-06-01T12:00:00.000Z", ...} for every <id>_model-1.jpeg
structure-images manifest --dest s3://images-a/structures/ --out /cache/pdb/manifest.json

# In each of the N parallel tasks
structure-images render \
  --ids /cache/pdb/pdbx_core_ids-1.txt \
  --model-url https://models.example.org/pdb \
  --dest s3://images-a/structures/ \
  --holdings /cache/pdb/holdings/released_structures_last_modified_dates.json.gz \
  --manifest /cache/pdb/manifest.json \
  --concurrency 8
```

`structure-images --help` lists every option. S3 settings come from the environment: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and `AWS_ENDPOINT_URL_S3` or `AWS_ENDPOINT_URL` (or pass `--endpoint`).

### Which ids get rendered

- Without `--holdings`, every id in the list is rendered.
- With `--holdings`, an id is skipped when `<id>_model-1.jpeg` exists in S3 and is at least as new as the holdings timestamp. An id that isn't in holdings is rendered only if its image is missing. This is the same rule the Python CLI used.
- `--manifest` answers "up to date" from the file without asking S3. Ids it doesn't show as current get one `HEAD` request to confirm, so an Airflow retry with an old manifest doesn't redo ids the first attempt already uploaded.
- Without `--manifest`, every id gets a `HEAD` request (32 at a time by default).

Each id's `_model-1.jpeg` is uploaded after its other images. If a task dies partway through an id, that id still looks missing and gets redone.

## Development

```bash
npm install --ignore-scripts   # skips compiling headless-gl, which the tests don't need
npm test
```
