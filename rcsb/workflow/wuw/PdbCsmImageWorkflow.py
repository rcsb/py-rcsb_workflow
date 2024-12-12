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

import gzip
import json
import datetime
import random
from pathlib import Path
import logging
import subprocess

logger = logging.getLogger(__name__)


class PdbCsmImageWorkflow:

    def getPdbList(self, **kwargs: dict) -> list:
        """Build pdb list via pdb gz file."""
        pdbIdsTimestamps = {}
        with gzip.open(kwargs.get("pdbGzPath")) as file:
            data = json.loads(file.read())
            for idVal in data:
                datetimeObject = datetime.datetime.strptime(data[idVal], "%Y-%m-%dT%H:%M:%S%z")
                pdbIdsTimestamps[idVal.lower()] = datetimeObject
        pdbIdList = []
        if kwargs.get("updateAllImages"):
            for idVal in pdbIdsTimestamps:
                path = idVal[1:3] + "/" + idVal + ".bcif"
                pdbIdList.append(f"{idVal} {path} experimental")
        else:
            for idVal, timestamp in pdbIdsTimestamps.items():
                path = idVal[1:3] + "/" + idVal + ".bcif"
                bcifFile = kwargs.get("pdbBaseDir") + path
                if Path.exists(bcifFile):
                    t1 = Path.stat(bcifFile).stMtime
                    t2 = timestamp.timestamp()
                    if t1 < t2:
                        pdbIdList.append(f"{idVal} {path} experimental")
                else:
                    pdbIdList.append(f"{idVal} {path} experimental")
        return pdbIdList

    def getCsmList(self, **kwargs: dict) -> list:
        """Build csm list via csm gz file."""
        with gzip.open(kwargs.get("csmGzPath")) as file:
            data = json.loads(file.read())
            dic = {}
            for modelId in data:
                item = data[modelId]
                item["modelPath"] = item["modelPath"].lower()  # prod route of BinaryCIF wf produces lowercase filenames
                item["datetime"] = datetime.datetime.strptime(item["lastModifiedDate"], "%Y-%m-%dT%H:%M:%S%z")
                dic[modelId.lower()] = item
        modelIdsMetadata = dic
        modelList = []

        if kwargs.get("updateAllImages"):
            for modelId, metadata in modelIdsMetadata.items():
                modelPath = metadata["modelPath"].replace(".cif", ".bcif").replace(".gz", "")
                modelList.append(f"{modelId} {modelPath} computational")
        else:
            # "incremental" for weekly
            for modelId, metadata in modelIdsMetadata.items():
                modelPath = metadata["modelPath"].replace(".cif", ".bcif").replace(".gz", "")
                bcifFile = kwargs.get("csmBaseDir") + modelPath
                if Path.exists(bcifFile):
                    t1 = Path.stat(bcifFile).stMtime
                    t2 = metadata["datetime"].timestamp()
                    if t1 < t2:
                        modelList.append(f"{modelId} {modelPath} computational")
                else:
                    modelList.append(f"{modelId} {modelPath} computational")
        return modelList

    def imagesGenLists(self, **kwargs: dict) -> None:
        """Generate lists of pdbs/csms in files."""
        pdbIdList = self.getPdbList(pdbGzPath=kwargs.get("pdbGzPath"),
                                    updateAllImages=kwargs.get("updateAllImages"),
                                    pdbBaseDir=kwargs.get("pdbBaseDir")
                                    )
        compIdList = [] if kwargs.get("imgsExcludeModels") else self.getCsmList(csmGzPath=kwargs.get("csmGzPath"),
                                                                                updateAllImages=kwargs.get("updateAllImages"),
                                                                                csmBaseDir=kwargs.get("csmBaseDir")
                                                                                )

        # Print results, combine, and shuffle
        if len(pdbIdList) < 1 and len(compIdList) < 1:
            raise ValueError("pdb and csm id list empty")

        fullIdList = pdbIdList + compIdList
        random.shuffle(fullIdList)

        steps = int(len(fullIdList) / int(kwargs.get("numWorkers")))
        for i in range(0, len(fullIdList), steps):
            Path(kwargs.get("idListPath")).mkdir(parents=True, exist_ok=True)
            with Path.open(kwargs.get("idListPath") + str(int(i/steps)), "w", encoding="utf-8") as file:
                for line in fullIdList[i: i + steps]:
                    file.write(line + "\n")

    def imagesGenJpgs(self, **kwargs: dict) -> None:
        """Generate jpgs for given pdb/csm list."""
        with Path.open(kwargs.get("idListPath") + kwargs.get("fileNumber"), "r", encoding="utf-8") as file:
            idList = [line.rstrip("\n") for line in file]
        if not isinstance(idList, list):
            raise TypeError("idList not a list")

        for line in idList:
            # Requirements:
            # 1. bcif files must be unzipped
            # 2. bcif files must be in a local dir (otherwise I"ll have to add a curl step)
            # 3. bcif files do not need conversion from cif files. This should be taken care of in bcif workflow
            #
            # add a step that checks:
            # 1. the dir exists
            # 2. the file is a bcif with data inside of it
            # 3. the output dir is availible

            fileId, bcifFileName, sdm = line.split(" ")
            contentTypeDir = "pdb/" if sdm == "experimental" else "csm/"

            bcifFilePath = kwargs.get("pdbBaseDir") + bcifFileName if sdm == "experimental" else kwargs.get("csmBaseDir") + bcifFileName

            outPath = kwargs.get("jpgsOutDir") + contentTypeDir
            Path(outPath).mkdir(parents=True, exist_ok=True)

            # runMolrender
            cmd = [
                kwargs.get("jpgXvfbExecutable"),
                "-a",
                "-s", f"-ac -screen 0 {kwargs.get('jpgScreen')}",
                kwargs.get("molrenderExe"),
                "all",
                bcifFilePath,
                outPath,
                "--height", kwargs.get("jpgHeight"),
                "--width", kwargs.get("jpgWidth"),
                "--format", kwargs.get("jpgFormat"),
            ]
            if kwargs.get("jpgAdditionalCmds") is None:
                cmd = [*cmd, kwargs.get("jpgAdditionalCmds")]
            try:
                result = subprocess.run(cmd, captureOutput=True, text=True, check=True)
                logger.info("Command was successful!")
                logger.info(result.stdout)
            except subprocess.CalledProcessError as e:
                msg = f"Command failed with exit code {e.returncode} \n Error output: {e.stderr}"
                logging.exception(msg)
                raise

            # check result
            outJpgFile = outPath + fileId + "Model-1.jpeg"

            if Path(outJpgFile).is_file() and Path(outJpgFile).stat().stSize > 0:
                logger.info("Got the image file %s.", outJpgFile)
            else:
                logger.warning("No image file: %s.", outJpgFile)

            # Potentially will need to zip afterwards.
            # This could be its own task
            # ZIP
            # cmd = ["gzip", "-f", outFile]
            # run(cmd)
