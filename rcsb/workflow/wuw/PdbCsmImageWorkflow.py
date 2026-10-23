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
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
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

        # get existing jpg timestamps in S3 #
        s3TimeStampDict = {}
        if s3Url and s3TimestampFile:
            s3TimeStampDict = self.readS3TimestampFile(s3TimestampFile, targetFileSuffix)
            logger.info("Read %s S3 jpg timestamps from %s", len(s3TimeStampDict), s3TimestampFile)

        # verify output is MISSING or OLD before including ID #
        idListToDo = []
        for line in idList:
            name = line.lower()  # for local jpg file
            nameHash = idHash(name)
            outPath = Path(jpgsOutDir) / nameHash / name
            target = outPath / (name + targetFileSuffix)
            if s3Url:
                t1 = s3TimeStampDict.get(name)
            else:
                t1 = target.stat().st_mtime if target.exists() else None
            if self.isOutOfDate(name, t1, holdingsFileDict):
                idListToDo = [*idListToDo, name]
        if s3Url and holdingsFileDict:
            # the timestamp file may predate uploads from an earlier attempt of this task, so confirm with S3 directly
            idListToDo = self.confirmS3OutOfDate(idListToDo, holdingsFileDict, s3Url, targetFileSuffix)
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

    def readS3TimestampFile(self, filePath, targetFileSuffix):
        """Read 's5cmd ls' output lines ('2026/10/20 14:03:11   52431  1abc_model-1.jpeg', UTC) into {name: epoch seconds}."""
        timeStampDict = {}
        with open(filePath, encoding="utf-8") as ifh:
            for line in ifh:
                fields = line.split()
                if len(fields) < 4 or not fields[-1].endswith(targetFileSuffix):
                    continue
                name = fields[-1].rpartition("/")[2][: -len(targetFileSuffix)].lower()
                lastModified = datetime.datetime.strptime(f"{fields[0]} {fields[1]}", "%Y/%m/%d %H:%M:%S").replace(tzinfo=datetime.timezone.utc)
                timeStampDict[name] = lastModified.timestamp()
        return timeStampDict

    def confirmS3OutOfDate(self, idList, holdingsFileDict, s3Url, targetFileSuffix, numThreads=32):
        """Re-check idList against the jpg timestamps in S3 (one HEAD request per id) and return the ids still out of date."""
        bucket, _, prefix = s3Url.partition("://")[2].strip("/").partition("/")
        client = boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL"))

        def getTimeStamp(name):
            key = "/".join(filter(None, [prefix, name + targetFileSuffix]))
            try:
                return client.head_object(Bucket=bucket, Key=key)["LastModified"].timestamp()
            except botocore.exceptions.ClientError as e:
                if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                    return None
                raise

        with ThreadPoolExecutor(max_workers=numThreads) as executor:
            timeStamps = list(executor.map(getTimeStamp, idList))
        idListToDo = [name for name, t in zip(idList, timeStamps) if self.isOutOfDate(name, t, holdingsFileDict)]
        logger.info("S3 check: %s of %s candidate ids are missing or out of date", len(idListToDo), len(idList))
        return idListToDo
