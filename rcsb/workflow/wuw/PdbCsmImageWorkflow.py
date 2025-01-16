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
import math


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


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
                path = idVal + ".bcif" if kwargs.get("noSubdirs") else idVal[1:3] + "/" + idVal + ".bcif"
                pdbIdList.append(f"{idVal} {path} experimental")
        else:
            for idVal, timestamp in pdbIdsTimestamps.items():
                path = idVal + ".bcif" if kwargs.get("noSubdirs") else idVal[1:3] + "/" + idVal + ".bcif"  # idVal[1:3] + "/" + idVal + ".bcif"
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
                                    pdbBaseDir=kwargs.get("pdbBaseDir"),
                                    noSubdirs=kwargs.get("noSubdirs"),
                                    )
        compIdList = [] if kwargs.get("imgsExcludeModels") else self.getCsmList(csmGzPath=kwargs.get("csmGzPath"),
                                                                                updateAllImages=kwargs.get("updateAllImages"),
                                                                                csmBaseDir=kwargs.get("csmBaseDir"),
                                                                                )

        # Print results, combine, and shuffle
        if len(pdbIdList) < 1 and len(compIdList) < 1:
            raise ValueError("pdb and csm id list empty")

        fullIdList = pdbIdList + compIdList
        random.shuffle(fullIdList)
        # logger.info('%s Ids split over %s files', len(fullIdList), kwargs.get("numWorkers"))

        # Calculate the size of each chunk
        chunkSize = math.ceil(len(fullIdList) / int(kwargs.get("numWorkers")))

        # Split the list into n chunks
        chunks = [fullIdList[i:i + chunkSize] for i in range(0, len(fullIdList), chunkSize)]

        # Write each chunk to a separate file
        Path(kwargs.get("idListPath")).mkdir(parents=True, exist_ok=True)
        for i, chunk in enumerate(chunks):
            filename = kwargs.get("idListPath") + f"idList_{i}.txt"
            logger.info('%s contains %s ids', f"idList_{i}.txt", len(chunk))
            with Path.open(filename, 'w', encoding="utf-8") as file:
                file.write("\n".join(chunk))  # Join the chunk items with newlines for readability
            if not (Path(filename).is_file() and Path(filename).stat().st_size > 0):
                logger.error('Missing or empty file %s', filename)

    def imagesGenJpgs(self, **kwargs: dict) -> None:
        """Generate jpgs for given pdb/csm list."""
        idListNumber = kwargs.get("fileNumber")
        idListFile = kwargs.get("idListPath") + f"idList_{idListNumber}.txt"

        if not (Path(idListFile).is_file() and Path(idListFile).stat().st_size > 0):
            logger.warning('Missing idList file %s', idListFile)
            return

        with Path.open(idListFile, "r", encoding="utf-8") as file:
            idList = [line.rstrip("\n") for line in file]
        if not isinstance(idList, list):
            raise TypeError("idList not a list")

        for line in idList:

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
                "--height", str(kwargs.get("jpgHeight")),
                "--width", str(kwargs.get("jpgWidth")),
                "--format", kwargs.get("jpgFormat"),
            ]
            if kwargs.get("jpgAdditionalCmds"):
                cmd = [*cmd, kwargs.get("jpgAdditionalCmds")]

            if Path(bcifFilePath).is_file() and Path(bcifFilePath).stat().st_size > 0:
                # logger.info('Running %s', ' '.join(cmd))
                try:
                    subprocess.run(cmd, capture_output=True, text=True, check=True)
                except subprocess.CalledProcessError:
                    logger.exception()

                # check result
                outJpgFile = outPath + fileId + "_model-1.jpeg"

                if not (Path(outJpgFile).is_file() and Path(outJpgFile).stat().st_size > 0):
                    logger.error("No image file: %s.", outJpgFile)
            else:
                logger.error('Missing bcif file %s', bcifFilePath)
