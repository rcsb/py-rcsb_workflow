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
        contentTypeDir = kwargs.get("contentTypeDir")
        numProcs = kwargs.get("numProcs")

        holdingsFilePath = Path(kwargs.get("holdingsFilePath"))
        targetFileSuffix = kwargs.get("targetFileSuffix")

        # load id list file
        logger.info("using id file %s", idListFile)
        listFileObj = Path(idListFile)
        if not (listFileObj.is_file() and listFileObj.stat().st_size > 0):
            logger.error("Missing idList file %s", idListFile)
            raise
        mU = MarshalUtil()
        idList = mU.doImport(idListFile, fmt="list")
        if not isinstance(idList, list) and not idList:
            raise TypeError("idList not a list or is empty.")

        # get holdings file dict for their timestamps
        if "computed-models-holdings-list" in str(holdingsFilePath):
            # the csm holdings file points to the actual holdings file
            holdingsFileDict = {}
            pointerDict = mU.doImport(holdingsFilePath, fmt="json")
            for key in pointerDict:
                holdingsFileDict.update(mU.doImport(str(holdingsFilePath.parent / key), fmt="json"))
        else:
            # pdb and ihm holdings file simply contain everything
            holdingsFileDict = mU.doImport(holdingsFilePath, fmt="json")

        ########################

        # generate list of commands
        argsL = []
        failedIds = []
        logger.info("Id list contains %s entries", len(idList))
        for i, line in enumerate(idList):
            name = line.lower()
            nameHash = idHash(name)

            bcifFileName = os.path.join(nameHash, name) + ".bcif.gz"
            logger.info("%s checking %s %s %s", i, name, bcifFileName, contentTypeDir)

            bcifFilePath = os.path.join(baseDir, bcifFileName)
            outPath = os.path.join(jpgsOutDir, contentTypeDir, nameHash, name)
            Path(outPath).mkdir(parents=True, exist_ok=True)

            if Path(outPath + targetFileSuffix).exists():
                ### get timestamp of 'line' from holdingsFileDict
                if isinstance(holdingsFileDict[line.upper()], dict):
                    # csm
                    timeStamp = holdingsFileDict[line.upper()]["lastModifiedDate"]
                else:
                    timeStamp = holdingsFileDict[line.upper()]
                # compare timestamps
                t1 = Path(outPath + targetFileSuffix).stat().st_mtime
                t2 = datetime.datetime.strptime(timeStamp, "%Y-%m-%dT%H:%M:%S%z").timestamp()
                if t1 < t2:
                    bcifFileObj = Path(bcifFilePath)
                    if bcifFileObj.is_file() and bcifFileObj.stat().st_size > 0:
                        cmd = [
                            jpgXvfbExecutable,
                            "-a",
                            "-s", f"-ac -screen 0 {jpgScreen}",
                            molrenderExe,
                            jpgRender,
                            bcifFilePath,
                            outPath,
                            "--height", jpgHeight,
                            "--width", jpgWidth,
                            "--format", jpgFormat,
                        ]
                        argsL.append((cmd, outPath, name, checkFileAppend))
                    else:
                        logger.error("Missing bcif file %s", bcifFilePath)
                        failedIds.append(name)

        # run on single cpu
        if numProcs == 1:
            for args in argsL:
                try:
                    self.run_command(args)
                except Exception as e:
                    logger.error("Failed to generate jpg for ID %s: %s", name, str(e))
                    failedIds.append(name)
        else:
            # run in parallel
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

        # raise if anything failed earlier
        if failedIds:
            logger.error("The following IDs failed to generate jpgs and will overwrite %s for later rerunning: %s", idListFile, failedIds)
            mU.doExport(idListFile, failedIds, fmt="list")
            raise RuntimeError(f"JPG generation failed for {len(failedIds)} IDs.")

    def run_command(self, args):
        """Run a command and verify the output file."""
        cmd, outPath, name, checkFileAppend = args
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info(f"[{name}] STDOUT:\n{result.stdout}\n[{name}] STDERR:\n{result.stderr}")
        except subprocess.CalledProcessError as e:
            print(f"[{name}] ERROR:\n{e.stderr}")
            raise

        # Verify output file
        outJpgFile = os.path.join(outPath, name + checkFileAppend)
        outFileObj = Path(outJpgFile)
        if not (outFileObj.is_file() and outFileObj.stat().st_size > 0):
            raise ValueError(f"No image file generated: {outJpgFile}")

        logger.info("Success: %r", cmd)
