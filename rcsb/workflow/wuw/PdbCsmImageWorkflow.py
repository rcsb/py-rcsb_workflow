"""Images workflow functions that actually do the work."""
##
# File: PdbCsmImageWorkflow.py
# Date: 11-Dec-2024  mjt
#
#  Pdb and Csm image generation  - for workflow pipeline
#
#  Updates:
#  12-Dec-2024 mjt Created class object
#  26-Oct-2026 mjt Optional direct upload to s3 (SeaweedFS) with s3 timestamp check
#
##
__docformat__ = "google en"
__author__ = "Michael Trumbull"
__email__ = "michael.trumbull@rcsb.org"
__license__ = "Apache 2.0"

from pathlib import Path
import logging
import mimetypes
import shutil
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import boto3
from botocore.exceptions import ClientError
import requests
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.workflow.wuw.WuwUtils import idHash
import datetime
import urllib

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


class PdbCsmImageWorkflow:

    def imagesGenJpgs(self, **kwargs: dict) -> None:
        """Generate jpgs for given pdb/csm list."""

        # kwargs #

        idListFile = kwargs.get("idListFilePath", None)
        jpgXvfbExecutable = kwargs.get("jpgXvfbExecutable")
        jpgScreen = kwargs.get("jpgScreen")
        molrenderExe = kwargs.get("molrenderExe")
        jpgRender = kwargs.get("jpgRender")
        jpgHeight = str(kwargs.get("jpgHeight"))
        jpgWidth = str(kwargs.get("jpgWidth"))
        jpgFormat = kwargs.get("jpgFormat")
        checkFileAppend = kwargs.get("checkFileAppend", "_model-1.jpeg")
        baseDir = kwargs.get("baseDir")
        baseUrl = kwargs.get("baseUrl")  # New: URL base for remote files
        jpgsOutDir = kwargs.get("jpgsOutDir")
        numProcs = kwargs.get("numProcs")
        holdingsFilePath = Path(kwargs.get("holdingsFilePath")) if kwargs.get("holdingsFilePath") else None
        targetFileSuffix = kwargs.get("targetFileSuffix")
        csmHoldingsFileSubstring = kwargs.get("csmHoldingsFileSubstring")
        modelFileExtension = kwargs.get("modelFileExtension")
        tmpDir = kwargs.get("tmpDir")
        s3Url = kwargs.get("s3Url")  # upload jpgs here (e.g. s3://images-a/structures/) instead of writing to jpgsOutDir

        if not (s3Url or jpgsOutDir):
            raise ValueError("One of jpgsOutDir or s3Url is required")
        s3Bucket = s3Prefix = None
        if s3Url:
            # s3://images-a/structures/ -> bucket 'images-a', key prefix 'structures/'
            s3Bucket, _, s3Prefix = s3Url.split("://", 1)[-1].strip("/").partition("/")
            s3Prefix = s3Prefix + "/" if s3Prefix else ""

        # load ID list file into memory #

        logger.info("using id file %s", idListFile)
        listFileObj = Path(idListFile)
        if not (listFileObj.is_file() and listFileObj.stat().st_size > 0):
            raise RuntimeError(f"Missing idList file {idListFile}")
        mU = MarshalUtil()
        idList = mU.doImport(idListFile, fmt="list")
        if not isinstance(idList, list) and not idList:
            raise TypeError("idList not a list or is empty.")
        logger.info("Full id list contains %s entries", len(idList))

        # get holdings file dict for timestamps #
        holdingsFileDict = {}
        if holdingsFilePath:
            if csmHoldingsFileSubstring in str(holdingsFilePath):
                # the csm holdings file points to the actual holdings file
                pointerDict = mU.doImport(str(holdingsFilePath), fmt="json")
                for key in pointerDict:
                    holdingsFileDict.update(mU.doImport(str(holdingsFilePath.parent / Path(key).name), fmt="json"))
            else:
                # pdb and ihm holdings file simply contain everything
                holdingsFileDict = mU.doImport(str(holdingsFilePath), fmt="json")

        # verify output is MISSING or OLD before including ID #
        if s3Url:
            # credentials and endpoint come from AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_ENDPOINT_URL
            s3 = boto3.client("s3")
        idListToDo = []
        for line in idList:
            name = line.lower()
            # t1 is the timestamp of the existing target jpg, None if it's missing
            if s3Url:
                # one HEAD request at a time, to keep the load on seaweed low
                try:
                    t1 = s3.head_object(Bucket=s3Bucket, Key=s3Prefix + name + targetFileSuffix)["LastModified"].timestamp()
                except ClientError as e:
                    if e.response["Error"]["Code"] not in ("404", "NoSuchKey", "NotFound"):
                        raise
                    t1 = None
            else:
                target = Path(jpgsOutDir) / idHash(name) / name / (name + targetFileSuffix)
                t1 = target.stat().st_mtime if target.exists() else None
            if t1 is not None and holdingsFileDict:
                # get timestamp of 'name' from holdingsFileDict
                if name.upper() in holdingsFileDict:
                    if isinstance(holdingsFileDict[name.upper()], dict):
                        # csm holdings file format
                        timeStamp = holdingsFileDict[name.upper()]["lastModifiedDate"]
                    else:
                        # pdb / ihm holdings file format
                        timeStamp = holdingsFileDict[name.upper()]
                    # compare timestamps
                    t2 = datetime.datetime.strptime(timeStamp, "%Y-%m-%dT%H:%M:%S%z").timestamp()
                    if t1 < t2:
                        idListToDo.append(name)
            else:
                idListToDo.append(name)
        logger.info("Delta ids (%s entries): %s", len(idListToDo), " ".join(idListToDo))

        # Generate commands tuple in form ProcessPoolExecutor accepts #
        argsL = []
        failedIds = []
        for line in idListToDo:
            name = line.lower()  # for local bcif file
            nameHash = idHash(name)
            if s3Url:
                outPath = Path(tmpDir) / "jpgs" / name  # scratch dir, removed after upload
            else:
                outPath = Path(jpgsOutDir) / nameHash / name  # jpg files are in a subdir of the name under the name hash
            # Handle optional hashing for input path
            if baseUrl:
                # URL-based source
                bcifSource = Path(tmpDir) / (name + modelFileExtension)
                bcifRemote = urllib.parse.urljoin(baseUrl, name + modelFileExtension)
            else:
                bcifSource = Path(baseDir) / nameHash / (name + modelFileExtension)
                bcifRemote = None
            outPath.mkdir(parents=True, exist_ok=True)
            # make sure a bcif file exists for this run (only for local files)
            if baseUrl or (bcifSource.is_file() and bcifSource.stat().st_size > 0):
                cmd = [
                    jpgXvfbExecutable,
                    "-a",
                    "-s", f"-ac -screen 0 {jpgScreen}",
                    molrenderExe,
                    jpgRender,
                    str(bcifSource),
                    outPath,
                    "--height", jpgHeight,
                    "--width", jpgWidth,
                    "--format", jpgFormat,
                ]
                argsL.append((cmd, outPath, name, checkFileAppend, bcifSource, bcifRemote, s3Bucket, s3Prefix))
            else:
                logger.error("Missing bcif file %s", bcifSource)
                failedIds.append(name)

        # run commands #
        if numProcs == 1:
            for argsTup in argsL:
                try:
                    self.run_command_with_url(argsTup)
                except Exception as e:
                    name = argsTup[2]
                    logger.error("Failed to generate jpg for ID %s: %s", name, str(e))
                    failedIds.append(name)
        else:
            with ProcessPoolExecutor(max_workers=int(numProcs)) as executor:
                future_to_name = {}
                for argsTup in argsL:
                    name = argsTup[2]
                    future = executor.submit(self.run_command_with_url, argsTup)
                    future_to_name[future] = name

                for future in as_completed(future_to_name):
                    name = future_to_name[future]
                    try:
                        future.result()
                    except Exception as e:
                        logger.error("Subprocess failed for ID %s: %s", name, str(e))
                        failedIds.append(name)

        # check for failures #
        if len(failedIds) > 0:
            logger.error("The following IDs failed to generate jpgs and will overwrite %s for later rerunning: %s", idListFile, failedIds)
            raise RuntimeError(f"JPG generation failed for {len(failedIds)} IDs.")

    def run_command_with_url(self, argsTup):
        """Run a command with URL support, downloading files to temp location if needed.
        If s3Bucket is set, upload the generated jpgs there and remove the local copies."""
        cmd, outPath, name, checkFileAppend, bcifSource, bcifRemote, s3Bucket, s3Prefix = argsTup
        try:
            # download to tmp dir if remote bcif file
            if bcifRemote:
                response = requests.get(bcifRemote, timeout=5)
                response.raise_for_status()
                tmpfile = Path(bcifSource)
                tmpfile.write_bytes(response.content)
            # run command
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info("%s: %s %s", name, result.stdout, result.stderr)

            # cleanup
            if bcifRemote:
                tmpfile.unlink()

            # Verify output file
            outJpgFile = os.path.join(outPath, name + checkFileAppend)
            outFileObj = Path(outJpgFile)
            if not (outFileObj.is_file() and outFileObj.stat().st_size > 0):
                raise ValueError(f"No image file generated: {outJpgFile}")

            # upload flattened, matching the s5cmd 'cp --flatten' layout (<prefix><file name>)
            if s3Bucket:
                s3 = boto3.client("s3")
                # upload the check file last so its timestamp only marks the id as done once everything is there
                jpgL = sorted(Path(outPath).glob("*" + outFileObj.suffix), key=lambda p: p.name == outFileObj.name)
                for jpg in jpgL:
                    contentType = mimetypes.guess_type(jpg.name)[0] or "application/octet-stream"
                    s3.upload_file(str(jpg), s3Bucket, s3Prefix + jpg.name, ExtraArgs={"ContentType": contentType})
                logger.info("%s: uploaded %d jpgs to s3://%s/%s", name, len(jpgL), s3Bucket, s3Prefix)

        except subprocess.CalledProcessError as e:
            logger.error("%s: %s", name, str(e))
            raise
        finally:
            if s3Bucket:
                shutil.rmtree(outPath, ignore_errors=True)
