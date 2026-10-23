"""Images workflow functions that actually do the work."""
##
# File: PdbCsmImageWorkflow.py
# Date: 11-Dec-2024  mjt
#
#  Pdb and Csm image generation  - for workflow pipeline
#
#  Updates:
#  12-Dec-2024 mjt Created class object
#
##
__docformat__ = "google en"
__author__ = "Michael Trumbull"
__email__ = "michael.trumbull@rcsb.org"
__license__ = "Apache 2.0"

from pathlib import Path
import logging
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import boto3
import botocore.exceptions
import requests
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.utils.io.S3Util import S3Util
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
        s3Url = kwargs.get("s3Url")
        s3TimestampFile = kwargs.get("s3TimestampFile")

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

        # get existing jpg timestamps in S3 (file written by genS3TimestampFile) #
        s3TimeStampDict = {}
        if s3Url and s3TimestampFile:
            with open(s3TimestampFile, encoding="utf-8") as ifh:
                for line in ifh:
                    name, timeStamp = line.split()
                    s3TimeStampDict[name] = datetime.datetime.strptime(timeStamp, "%Y-%m-%dT%H:%M:%S%z").timestamp()

        # verify output is MISSING or OLD before including ID #
        idListToDo = []
        for line in idList:
            name = line.lower()  # for local jpg file
            nameHash = idHash(name)
            outPath = Path(jpgsOutDir) / nameHash / name
            target = outPath / (name + targetFileSuffix)
            if s3Url:
                t1 = s3TimeStampDict.get(name)
                if holdingsFileDict and self.isOutOfDate(name, t1, holdingsFileDict):
                    # the timestamp file may predate uploads from an earlier attempt of this task, so ask S3 directly
                    t1 = self.getS3TimeStamp(s3Url, name + targetFileSuffix)
            else:
                t1 = target.stat().st_mtime if target.exists() else None
            if self.isOutOfDate(name, t1, holdingsFileDict):
                idListToDo = [*idListToDo, name]
        logger.info("Delta ids (%s entries): %s", len(idListToDo), " ".join(idListToDo))

        # Generate commands tuple in form ProcessPoolExecutor accepts #
        argsL = []
        failedIds = []
        for line in idListToDo:
            name = line.lower()  # for local bcif file
            nameHash = idHash(name)
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
                argsL.append((cmd, outPath, name, checkFileAppend, bcifSource, bcifRemote, s3Url))
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
        """Run a command with URL support, downloading files to temp location if needed."""
        cmd, outPath, name, checkFileAppend, bcifSource, bcifRemote, s3Url = argsTup
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

        except subprocess.CalledProcessError as e:
            logger.error("%s: %s", name, str(e))
            raise

        # Verify output file
        outJpgFile = os.path.join(outPath, name + checkFileAppend)
        outFileObj = Path(outJpgFile)
        if not (outFileObj.is_file() and outFileObj.stat().st_size > 0):
            raise ValueError(f"No image file generated: {outJpgFile}")

        # upload flat to S3, check file last so a partial upload still looks missing
        if s3Url:
            s3Util = S3Util(s3Url)
            fileNames = sorted(os.listdir(outPath), key=lambda fileName: fileName == name + checkFileAppend)
            for fileName in fileNames:
                s3Util.storeBundle(os.path.join(outPath, fileName), "", fileName)

    def isOutOfDate(self, name, targetTimeStamp, holdingsFileDict):
        """True if the jpg for name is missing (targetTimeStamp None) or older than its holdings timestamp. Always True without holdings."""
        if targetTimeStamp is None or not holdingsFileDict:
            return True
        # get timestamp of 'name' from holdingsFileDict
        if name.upper() not in holdingsFileDict:
            return False
        if isinstance(holdingsFileDict[name.upper()], dict):
            # csm holdings file format
            timeStamp = holdingsFileDict[name.upper()]["lastModifiedDate"]
        else:
            # pdb / ihm holdings file format
            timeStamp = holdingsFileDict[name.upper()]
        # compare timestamps
        return targetTimeStamp < datetime.datetime.strptime(timeStamp, "%Y-%m-%dT%H:%M:%S%z").timestamp()

    def genS3TimestampFile(self, **kwargs: dict) -> None:
        """Write '<id> <lastModified>' lines for every '<id><targetFileSuffix>' jpg under s3Url to s3TimestampFile."""
        s3Url = kwargs.get("s3Url")
        s3TimestampFile = kwargs.get("s3TimestampFile")
        targetFileSuffix = kwargs.get("targetFileSuffix")
        bucket, _, prefix = s3Url.partition("://")[2].strip("/").partition("/")
        prefix = prefix + "/" if prefix else ""
        client = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
        with open(s3TimestampFile, "w", encoding="utf-8") as ofh:
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    fileName = obj["Key"].removeprefix(prefix)
                    if fileName.endswith(targetFileSuffix):
                        ofh.write(f"{fileName.removesuffix(targetFileSuffix)} {obj['LastModified'].strftime('%Y-%m-%dT%H:%M:%S%z')}\n")
        logger.info("Wrote jpg timestamps from %s to %s", s3Url, s3TimestampFile)

    def getS3TimeStamp(self, s3Url, fileName):
        """Last-modified time (epoch seconds) of fileName under s3Url, or None if it doesn't exist."""
        bucket, _, prefix = s3Url.partition("://")[2].strip("/").partition("/")
        client = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))
        try:
            return client.head_object(Bucket=bucket, Key="/".join(filter(None, [prefix, fileName])))["LastModified"].timestamp()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                return None
            raise
