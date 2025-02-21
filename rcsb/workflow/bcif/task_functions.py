##
# File:    task_functions.py
# Author:  James Smith
# Date:    21-Feb-2025
##

"""
Workflow task descriptors that should be compliant with both the airflow scheduler and the command line interface.
"""

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import multiprocessing
import os
import shutil
import glob
import datetime
import pickle
import logging
from typing import List
import requests
from mmcif.api.DictionaryApi import DictionaryApi
from mmcif.io.IoAdapterPy import IoAdapterPy as IoAdapter
from rcsb.workflow.bcif.bcif_workflow_utilities import BcifWorkflowUtilities
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


def statusStart(listFileBase: str, statusStartFile: str) -> bool:
    startFile = os.path.join(listFileBase, statusStartFile)
    dirs = os.path.dirname(startFile)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
    with open(startFile, "w") as w:
        w.write("Binary cif run started at %s." % str(datetime.datetime.now()))
    return True


def makeDirs(workflowUtility: BcifWorkflowUtilities) -> bool:
    """mounted paths must be already made"""
    if not os.path.exists(workflowUtility.updateBase):
        os.makedirs(workflowUtility.updateBase, mode=0o777)
    for contentType in workflowUtility.contentTypeDir.values():
        path = os.path.join(workflowUtility.updateBase, contentType)
        if not os.path.exists(path):
            os.mkdir(path, mode=0o777)
    return True


def getPdbList(
    workflowUtility: BcifWorkflowUtilities,
    loadType: str,
    listFileBase: str,
    pdbListFileName: str,
    result=None,
) -> bool:
    outfile = os.path.join(listFileBase, pdbListFileName)
    if os.path.exists(outfile):
        return True
    # list[str]
    # 'pdb_id partial_path contentType'
    pdbList = workflowUtility.getPdbList(loadType)
    with open(outfile, "wb") as w:
        pickle.dump(pdbList, w)
    return True


def getCsmList(
    workflowUtility: BcifWorkflowUtilities,
    loadType: str,
    listFileBase: str,
    csmListFileName: str,
    result=None,
) -> bool:
    outfile = os.path.join(listFileBase, csmListFileName)
    if os.path.exists(outfile):
        return True
    # list[str]
    # 'pdb_id partial_path contentType'
    csmList = workflowUtility.getCompList(loadType)
    if not csmList:
        return False
    with open(outfile, "wb") as w:
        pickle.dump(csmList, w)
    return True


def makeTaskListFromRemote(
    listFileBase: str,
    pdbListFileName: str,
    csmListFileName: str,
    inputListFileName: str,
    maxfiles: int,
    workflowUtility: BcifWorkflowUtilities,
    result=None,
) -> bool:
    # read pdb list
    pdbList = None
    with open(os.path.join(listFileBase, pdbListFileName), "rb") as r:
        pdbList = pickle.load(r)
    if not pdbList:
        logger.error("error reading pdb list")
        return False
    # read csm list
    csmList = None
    csmListPath = os.path.join(listFileBase, csmListFileName)
    if os.path.exists(csmListPath):
        with open(csmListPath, "rb") as r:
            csmList = pickle.load(r)
    if not csmList:
        logger.error("error reading csm list")
    else:
        # join lists
        pdbList.extend(csmList)
    # trim list if testing
    nfiles = len(pdbList)
    logger.info("found %d cif files", nfiles)
    if nfiles == 0:
        return False
    if 0 < maxfiles < nfiles:
        nfiles = maxfiles
        pdbList = pdbList[0:nfiles]
        logger.info("reading only %d files", nfiles)
    # save input list
    outfile = os.path.join(listFileBase, inputListFileName)
    with open(outfile, "wb") as w:
        pickle.dump(pdbList, w)
    return True


def makeTaskListFromLocal(
    localDataPath: str, listFileBase: str, inputListFileName: str, result=None
) -> bool:
    """
    requires cif files in source folder with no subdirs
    writes to target folder with no subdirs
    """
    # traverse local folder
    tasklist = glob.glob(os.path.join(localDataPath, "*.cif.gz"))
    nfiles = len(tasklist)
    if nfiles == 0:
        tasklist = glob.glob(os.path.join(localDataPath, "*.cif"))
        nfiles = len(tasklist)
    logger.info("found %d cif files", nfiles)
    with open(os.path.join(listFileBase, inputListFileName), "wb") as w:
        pickle.dump(tasklist, w)
    return True


def splitTasks(
    listFileBase: str,
    inputListFileName: str,
    inputList2d: str,
    maxfiles: int,
    subtasks: int,
    result=None,
) -> List[int]:
    # read task list
    tasklist = None
    with open(os.path.join(listFileBase, inputListFileName), "rb") as r:
        tasklist = pickle.load(r)
    if not tasklist:
        logger.error("error reading task list")
        return None
    # trim list
    nfiles = len(tasklist)
    logger.info("found %d cif files", nfiles)
    if nfiles == 0:
        return []
    if 0 < maxfiles < nfiles:
        nfiles = maxfiles
        logger.info("reading only %d files", nfiles)
    # divide into subtasks
    if (subtasks is None) or not str(subtasks).isdigit():
        subtasks = 1
    subtasks = int(subtasks)
    if subtasks == 0:
        subtasks = multiprocessing.cpu_count()
        logger.info("machine has %d processors", subtasks)
    else:
        logger.info("dividing across %d subtasks", subtasks)
    tasks = splitList(nfiles, subtasks, tasklist)
    # save full tasks file
    logger.info("get local tasks saving %d tasks to %s", len(tasks), inputList2d)
    with open(os.path.join(listFileBase, inputList2d), "wb") as w:
        pickle.dump(tasks, w)
    # return list of task indices
    tasks = list(range(0, len(tasks)))
    logger.info("returning %d tasks", len(tasks))
    return tasks


def splitList(nfiles: int, subtasks: int, tasklist: List[str]) -> List[List[str]]:
    step = nfiles // subtasks
    if step < 1:
        step = 1
    steps = nfiles // step
    logger.info(
        "split list has %d files and %d steps with step %d", nfiles, steps, step
    )
    if not isinstance(tasklist[0], str):
        tasklist = [str(task) for task in tasklist]
    tasks = [
        (
            tasklist[index * step : step + index * step]
            if index < steps - 1
            else tasklist[index * step : nfiles]
        )
        for index in range(0, steps)
    ]
    return tasks


def localTaskMap(
    index: int,
    *,
    listFileBase: str = None,
    inputList2d: str = None,
    tempPath: str = None,
    updateBase: str = None,
    compress: bool = False,
    localInputsOrRemote: str = None,
    batch: int = 1,
    pdbxDict: str = None,
    maDict: str = None,
    rcsbDict: str = None,
    workflowUtility: BcifWorkflowUtilities = None
) -> bool:
    # read sublist
    infiles = None
    with open(os.path.join(listFileBase, inputList2d), "rb") as r:
        allfiles = pickle.load(r)
        infiles = allfiles[index]
    if not infiles:
        logger.error("error - no infiles")
        return False
    logger.info("task map has %d infiles", len(infiles))

    # form dictionary object
    dictionaryApi = None
    paths = [pdbxDict, maDict, rcsbDict]
    try:
        adapter = IoAdapter(raiseExceptions=True)
        containers = []
        for path in paths:
            containers += adapter.readFile(inputFilePath=path)
        dictionaryApi = DictionaryApi(containerList=containers, consolidate=True)
    except Exception as e:
        logger.exception("failed to create dictionary api: %s", str(e))

    # traverse sublist and send each input file to converter
    if (batch is None) or not str(batch).isdigit():
        batch = 1
    batch = int(batch)
    if batch == 0:
        batch = multiprocessing.cpu_count()
    procs = []
    if batch == 1:
        # process one file at a time
        for line in infiles:
            args = (
                line,
                localInputsOrRemote,
                updateBase,
                compress,
                tempPath,
                workflowUtility,
                dictionaryApi,
            )
            singleTask(*args)
    else:
        # process with file batching
        nfiles = len(infiles)
        tasks = splitList(nfiles, batch, infiles)
        for task in tasks:
            args = (
                task,
                localInputsOrRemote,
                updateBase,
                compress,
                tempPath,
                workflowUtility,
                dictionaryApi,
            )
            p = multiprocessing.Process(target=batchTask, args=args)
            procs.append(p)
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        procs.clear()

    return True


def batchTask(
    tasks,
    localInputsOrRemote,
    updateBase,
    compress,
    tempPath,
    workflowUtility,
    dictionaryApi,
):
    for task in tasks:
        singleTask(
            task,
            localInputsOrRemote,
            updateBase,
            compress,
            tempPath,
            workflowUtility,
            dictionaryApi,
        )


def singleTask(
    line,
    localInputsOrRemote,
    updateBase,
    compress,
    tempPath,
    workflowUtility,
    dictionaryApi,
):
    """
    download to cifFilePath
    form output path bcifFilePath
    """
    if localInputsOrRemote == "local":
        cifFilePath = line
        if (
            not os.path.exists(cifFilePath)
            and not os.path.exists(cifFilePath.replace(".gz", ""))
            and not os.path.exists("%s.gz" % cifFilePath)
        ):
            logger.error("error - could not find %s", cifFilePath)
            return
        pdbFileName = os.path.basename(cifFilePath)
        bcifFilePath = os.path.join(
            updateBase,
            pdbFileName.replace(".cif.gz", ".bcif").replace(".cif", ".bcif"),
        )
        if compress:
            bcifFilePath = "%s.gz" % bcifFilePath
        logger.info("converting %s to %s", cifFilePath, bcifFilePath)
    else:
        tokens = line.split(" ")
        dividedPath = tokens[1]
        enumType = tokens[2]
        pdbFileName = os.path.basename(dividedPath)
        cifFilePath = os.path.join(tempPath, pdbFileName)
        contentType = workflowUtility.contentTypeDir[enumType]
        bcifFilePath = os.path.join(
            updateBase,
            contentType,
            dividedPath.replace(".cif.gz", ".bcif").replace(".cif", ".bcif"),
        )
        if compress:
            bcifFilePath = "%s.gz" % bcifFilePath
        url = workflowUtility.getDownloadUrl(dividedPath, enumType)
        try:
            r = requests.get(url, timeout=300, stream=True)
            if r and r.status_code < 400:
                dirs = os.path.dirname(cifFilePath)
                if not os.path.exists(dirs):
                    os.makedirs(dirs, mode=0o777)
                    shutil.chown(dirs, "root", "root")
                with open(cifFilePath, "ab") as w:
                    for chunk in r.raw.stream(1024, decode_content=False):
                        if chunk:
                            w.write(chunk)
                shutil.chown(cifFilePath, "root", "root")
                os.chmod(cifFilePath, 0o777)
            else:
                raise requests.exceptions.RequestException(
                    "error - request failed for %s" % url
                )
        except Exception as e:
            logger.exception(str(e))
            if os.path.exists(cifFilePath):
                os.unlink(cifFilePath)
            return
    if os.path.exists(bcifFilePath):
        logger.info("file %s already exists", bcifFilePath)
        if localInputsOrRemote == "remote":
            os.unlink(cifFilePath)
        return
    # make nested directories
    dirs = os.path.dirname(bcifFilePath)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
        shutil.chown(dirs, "root", "root")
        os.chmod(dirs, 0o777)
    # convert to bcif
    try:
        result = bcifconvert(cifFilePath, bcifFilePath, tempPath, dictionaryApi)
        if not result:
            raise Exception("failed to convert %s" % cifFilePath)
        shutil.chown(bcifFilePath, "root", "root")
        os.chmod(bcifFilePath, 0o777)
    except Exception as e:
        logger.exception(str(e))
    # remove input file
    finally:
        if localInputsOrRemote == "remote":
            os.unlink(cifFilePath)


def validateOutput(
    *,
    listFileBase: str = None,
    inputListFileName: str = None,
    updateBase: str = None,
    compress: bool = None,
    missingFileBase: str = None,
    missingFileName: str = None,
    workflowUtility: BcifWorkflowUtilities = None,
    result=None
) -> bool:
    inputListFile = os.path.join(listFileBase, inputListFileName)
    if not os.path.exists(inputListFile):
        return False
    missing = []
    with open(inputListFile, "rb") as r:
        data = pickle.load(r)
        for line in data:
            dividedPath = line.split()[1]
            contentType = workflowUtility.contentTypeDir[line.split()[2]]
            basename = os.path.join(updateBase, contentType)
            filepath = os.path.join(basename, dividedPath)
            out = filepath.replace(".cif.gz", ".bcif").replace(".cif", ".bcif")
            if compress:
                out = "%s.gz" % out
            if not os.path.exists(out):
                missing.append(out)
    if len(missing) > 0:
        missingFile = os.path.join(missingFileBase, missingFileName)
        with open(missingFile, "w") as w:
            for line in missing:
                w.write(line)
    return True


def removeTempFiles(tempPath: str, listFileBase: str, result=None) -> bool:
    if not os.path.exists(tempPath):
        return False
    try:
        for filename in os.listdir(tempPath):
            path = os.path.join(tempPath, filename)
            if os.path.isfile(path):
                os.unlink(path)
        for filename in os.listdir(listFileBase):
            path = os.path.join(listFileBase, filename)
            if os.path.isfile(path):
                os.unlink(path)
    except Exception as e:
        logger.warning(str(e))
    return True


def tasksDone(result=None) -> bool:
    logger.info("task maps completed")
    return True


def k8sBranch() -> bool:
    logger.info("using k8s tasks")
    return True


def statusComplete(listFileBase: str, statusCompleteFile: str) -> bool:
    """
    must occur after end_task
    """
    completeFile = os.path.join(listFileBase, statusCompleteFile)
    dirs = os.path.dirname(completeFile)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
    with open(completeFile, "w") as w:
        w.write(
            "Binary cif run completed successfully at %s."
            % str(datetime.datetime.now())
        )
    return True


def bcifconvert(
    infile: str, outfile: str, workpath: str, dictionaryApi: DictionaryApi
) -> bool:
    mu = MarshalUtil(workPath=workpath)
    data = mu.doImport(infile, fmt="mmcif")
    try:
        result = mu.doExport(outfile, data, fmt="bcif", dictionaryApi=dictionaryApi)
        if not result:
            raise Exception()
    except Exception as e:
        logger.exception("error during bcif conversion: %s", str(e))
        return False
    return True
