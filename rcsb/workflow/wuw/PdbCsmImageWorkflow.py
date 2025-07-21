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
from rcsb.utils.io.MarshalUtil import MarshalUtil
from rcsb.workflow.wuw.WuwUtils import idHash
import datetime

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
        jpgsOutDir = kwargs.get("jpgsOutDir")
        numProcs = kwargs.get("numProcs")
        holdingsFilePath = Path(kwargs.get("holdingsFilePath"))
        targetFileSuffix = kwargs.get("targetFileSuffix")
        csmHoldingsFileSubstring = kwargs.get("csmHoldingsFileSubstring")

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

        if csmHoldingsFileSubstring in str(holdingsFilePath):
            # the csm holdings file points to the actual holdings file
            holdingsFileDict = {}
            pointerDict = mU.doImport(str(holdingsFilePath), fmt="json")
            for key in pointerDict:
                holdingsFileDict.update(mU.doImport(str(holdingsFilePath.parent / Path(key).name), fmt="json"))
        else:
            # pdb and ihm holdings file simply contain everything
            holdingsFileDict = mU.doImport(str(holdingsFilePath), fmt="json")

        # verify output is MISSING or OLD before including ID #
        idListToDo = []
        for line in idList:
            name = line.lower()
            nameHash = idHash(name)
            outPath = Path(jpgsOutDir) / nameHash / name
            target = outPath / (name + targetFileSuffix)
            if target.exists():
                # get timestamp of 'name' from holdingsFileDict
                if isinstance(holdingsFileDict[name.upper()], dict):
                    # csm holdings file format
                    timeStamp = holdingsFileDict[name.upper()]["lastModifiedDate"]
                else:
                    # pdb / ihm holdings file format
                    timeStamp = holdingsFileDict[name.upper()]
                # compare timestamps
                t1 = target.stat().st_mtime
                t2 = datetime.datetime.strptime(timeStamp, "%Y-%m-%dT%H:%M:%S%z").timestamp()
                if t1 < t2:
                    idListToDo = [*idListToDo, name]
            else:
                idListToDo = [*idListToDo, name]
        logger.info("Delta ids (%s entries): %s", len(idListToDo), " ".join(idListToDo))

        # Generate commands tuple in form ProcessPoolExecutor accepts #
        argsL = []
        failedIds = []
        for line in idListToDo:
            name = line.lower()
            nameHash = idHash(name)
            bcifFilePath = Path(baseDir) / nameHash / (name + ".bcif.gz")  # bcif files are directly under the namehash
            outPath = Path(jpgsOutDir) / nameHash / name  # jpg files are in a subdir of the name under the name hash
            outPath.mkdir(parents=True, exist_ok=True)
            # make sure a bcif file exists for this run
            if bcifFilePath.is_file() and bcifFilePath.stat().st_size > 0:
                cmd = [
                    jpgXvfbExecutable,
                    "-a",
                    "-s", f"-ac -screen 0 {jpgScreen}",
                    molrenderExe,
                    jpgRender,
                    str(bcifFilePath),
                    outPath,
                    "--height", jpgHeight,
                    "--width", jpgWidth,
                    "--format", jpgFormat,
                ]
                argsL.append((cmd, outPath, name, checkFileAppend))
            else:
                logger.error("Missing bcif file %s", bcifFilePath)
                failedIds.append(name)

        # run commands #
        if numProcs == 1:
            for args in argsL:
                try:
                    self.run_command(args)
                except Exception as e:
                    logger.error("Failed to generate jpg for ID %s: %s", name, str(e))
                    failedIds.append(name)
        else:
            with ProcessPoolExecutor(max_workers=int(numProcs)) as executor:
                future_to_name = {}
                for args in argsL:
                    name = args[2]
                    future = executor.submit(self.run_command, args)
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

    def run_command(self, args):
        """Run a command and verify the output file."""
        cmd, outPath, name, checkFileAppend = args
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info("%s: %s %s", name, result.stdout, result.stderr)
        except subprocess.CalledProcessError as e:
            logger.error("%s: %s", name, e.stderr)
            raise

        # Verify output file
        outJpgFile = os.path.join(outPath, name + checkFileAppend)
        outFileObj = Path(outJpgFile)
        if not (outFileObj.is_file() and outFileObj.stat().st_size > 0):
            raise ValueError(f"No image file generated: {outJpgFile}")
