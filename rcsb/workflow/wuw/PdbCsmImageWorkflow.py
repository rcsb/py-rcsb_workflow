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
        pdbBaseDir = kwargs.get("pdbBaseDir")
        csmBaseDir = kwargs.get("csmBaseDir")
        jpgsOutDir = kwargs.get("jpgsOutDir")
        contentTypeDir = kwargs.get("contentTypeDir")
        numProcs = kwargs.get("numProcs")

        logger.info("using id file %s", idListFile)

        listFileObj = Path(idListFile)
        if not (listFileObj.is_file() and listFileObj.stat().st_size > 0):
            logger.error("Missing idList file %s", idListFile)
            return

        mU = MarshalUtil()
        idList = mU.doImport(idListFile, fmt="list")
        if not isinstance(idList, list) and not idList:
            raise TypeError("idList not a list or is empty.")

        # generate list of commands
        argsL = []
        logger.info("Id list contains %s entries", len(idList))
        for i, line in enumerate(idList):
            name = line.lower()
            nameHash = idHash(name)

            bcifFileName = os.path.join(nameHash, name) + ".bcif.gz"
            logger.info("%s running %s %s %s", i, name, bcifFileName, contentTypeDir)

            if contentTypeDir == "pdb":
                bcifFilePath = os.path.join(pdbBaseDir, bcifFileName)
            else:
                bcifFilePath = os.path.join(csmBaseDir, bcifFileName)

            outPath = os.path.join(jpgsOutDir, contentTypeDir, nameHash, name)
            Path(outPath).mkdir(parents=True, exist_ok=True)

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
                raise ValueError(f"Missing bcif file {bcifFilePath}")
        if numProcs == 1:
            for args in argsL:
                self.run_command(args)
        else:
            with ProcessPoolExecutor(max_workers=int(numProcs)) as executor:
                futures = [executor.submit(self.run_command, args) for args in argsL]
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logger.error("Subprocess failed: %s", str(e))

    def run_command(self, args):
        """Run a command and verify the output file."""
        cmd, outPath, name, checkFileAppend = args
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            print(f"[{name}] STDOUT:\n{result.stdout}")
            print(f"[{name}] STDERR:\n{result.stderr}")
        except subprocess.CalledProcessError as e:
            print(f"[{name}] ERROR:\n{e.stderr}")
            raise

        # Verify output file
        outJpgFile = os.path.join(outPath, name + checkFileAppend)
        outFileObj = Path(outJpgFile)
        if not (outFileObj.is_file() and outFileObj.stat().st_size > 0):
            raise ValueError(f"No image file generated: {outJpgFile}")

        logger.info("Success: %r", cmd)
