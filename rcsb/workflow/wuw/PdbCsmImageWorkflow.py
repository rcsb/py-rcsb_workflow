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
from concurrent.futures import ProcessPoolExecutor
import os

from rcsb.utils.io.MarshalUtil import MarshalUtil

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


# For pickling reasons this command needs to not accept self
def run_command(args):
    """Run a command and verify the output file."""
    cmd, outPath, name, checkFileAppend = args
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        logger.info(result.stdout)
    except subprocess.CalledProcessError as e:
        logger.error("Error: %s", e)
        logger.error("Stderr: %s", e.stderr)
        raise

    # Verify output file
    outJpgFile = os.path.join(outPath, name + checkFileAppend)
    outFileObj = Path(outJpgFile)
    if not (outFileObj.is_file() and outFileObj.stat().st_size > 0):
        raise ValueError(f"No image file generated: {outJpgFile}")

    return f"Success: {cmd}"


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
        useIdSubdir = kwargs.get("useIdSubdir")
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
        args = []
        for line in idList:
            name = line.lower()
            if useIdSubdir:
                name = os.path.join(name[1:3], name)

            bcifFileName = name + ".bcif"
            logger.info("Running %s %s %s", name, bcifFileName, contentTypeDir)

            if contentTypeDir == "pdb":
                bcifFilePath = os.path.join(pdbBaseDir, bcifFileName)
            else:
                bcifFilePath = os.path.join(csmBaseDir, bcifFileName)

            outPath = os.path.join(jpgsOutDir, contentTypeDir)
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
                # passing in args this way is due to a pickling issue with multiple subprocesses
                args.append((cmd, outPath, name, checkFileAppend))
            else:
                raise ValueError(f"Missing bcif file {bcifFilePath}")
        if numProcs == 1:
            run_command(args[0])
        else:
            # Execute commands in parallel with process-based execution
            with ProcessPoolExecutor(max_workers=int(numProcs)) as executor:
                results = executor.map(run_command, args)
            # Print results
            for result in results:
                logger.info(result)
