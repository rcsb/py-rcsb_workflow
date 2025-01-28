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
import os

from rcsb.utils.io.MarshalUtil import MarshalUtil

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

        logger.info("using id file %s", idListFile)

        listFileObj = Path(idListFile)
        if not (listFileObj.is_file() and listFileObj.stat().st_size > 0):
            logger.error("Missing idList file %s", idListFile)
            return

        mU = MarshalUtil()
        idList = mU.doImport(idListFile, fmt="list")
        if not isinstance(idList, list) and not idList:
            raise TypeError("idList not a list or is empty.")

        for line in idList:

            fileId, bcifFileName, sdm = line.split()
            contentTypeDir = "pdb" if sdm == "experimental" else "csm"
            logger.info("Running %s %s %s", fileId, bcifFileName, sdm)

            if sdm == "experimental":
                bcifFilePath = os.path.join(pdbBaseDir, bcifFileName)
            else:
                bcifFilePath = os.path.join(csmBaseDir, bcifFileName)

            outPath = os.path.join(jpgsOutDir, contentTypeDir)
            Path(outPath).mkdir(parents=True, exist_ok=True)

            # runMolrender
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
            bcifFileObj = Path(bcifFilePath)
            if bcifFileObj.is_file() and bcifFileObj.stat().st_size > 0:
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                    logger.info(result.stdout)
                except subprocess.CalledProcessError as e:
                    logger.error("Error: %s", e)
                    logger.error("Stderr: %s", e.stderr)
                    raise

                # check result
                outJpgFile = os.path.join(outPath, fileId + checkFileAppend)
                outFileObj = Path(outJpgFile)
                if not (outFileObj.is_file() and outFileObj.stat().st_size > 0):
                    raise ValueError(f"No image file: {outJpgFile}")
            else:
                raise ValueError(f"Missing bcif file {bcifFilePath}")
