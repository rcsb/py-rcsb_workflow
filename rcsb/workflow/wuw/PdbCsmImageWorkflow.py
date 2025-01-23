"""Images workflow functions that actually do the work."""
##
# File: PdbCsmImageWorkflow.py
# Date: 11-Dec-2024  mjt
#
#  Pdb and Csm image generation  - for workflow pipeline
#
#  Updates:
#  12-dec-2024 mjt Created class object
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


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


class PdbCsmImageWorkflow:

    def imagesGenJpgs(self, **kwargs: dict) -> None:
        """Generate jpgs for given pdb/csm list."""
        idListName = kwargs.get("idListName")
        idListFile = os.path.join(kwargs.get("idListPath"), idListName)
        logger.info('using id file %s', idListFile)

        if not (Path(idListFile).is_file() and Path(idListFile).stat().st_size > 0):
            logger.warning('Missing idList file %s', idListFile)
            return

        with Path(idListFile).open("r", encoding="utf-8") as file:
            idList = [line.rstrip("\n") for line in file]
        if not isinstance(idList, list):
            raise TypeError("idList not a list")

        for line in idList:

            fileId, bcifFileName, sdm = line.split(" ")
            contentTypeDir = "pdb/" if sdm == "experimental" else "csm/"
            logger.info('Running %s %s %s', fileId, bcifFileName, sdm)

            bcifFilePath = os.path.join(kwargs.get("pdbBaseDir"), bcifFileName) if sdm == "experimental" else os.path.join(kwargs.get("csmBaseDir"), bcifFileName)

            outPath = os.path.join(kwargs.get("jpgsOutDir"), contentTypeDir)
            Path(outPath).mkdir(parents=True, exist_ok=True)

            # runMolrender
            cmd = [
                kwargs.get("jpgXvfbExecutable"),
                "-a",
                "-s", f"-ac -screen 0 {kwargs.get('jpgScreen')}",
                kwargs.get("molrenderExe"),
                kwargs.get("jpgRender"),
                bcifFilePath,
                outPath,
                "--height", str(kwargs.get("jpgHeight")),
                "--width", str(kwargs.get("jpgWidth")),
                "--format", kwargs.get("jpgFormat"),
            ]
            if kwargs.get("jpgAdditionalCmds"):
                cmd = [*cmd, kwargs.get("jpgAdditionalCmds")]

            if Path(bcifFilePath).is_file() and Path(bcifFilePath).stat().st_size > 0:
                # logger.info('Running %s', ' '.join(cmd))
                try:
                    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
                    logger.info(result.stdout)
                except subprocess.CalledProcessError as e:
                    logger.error("Error: %s", e)
                    logger.error("Stderr: %s", e.stderr)

                # check result
                outJpgFile = os.path.join(outPath, fileId + "_model-1.jpeg")

                if not (Path(outJpgFile).is_file() and Path(outJpgFile).stat().st_size > 0):
                    logger.error("No image file: %s.", outJpgFile)
            else:
                logger.error('Missing bcif file %s', bcifFilePath)
