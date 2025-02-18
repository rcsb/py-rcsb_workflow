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
from rcsb.workflow.bcif.workflow_functions import WorkflowUtilities
from rcsb.workflow.bcif.bcif_functions import bcifconvert

# pylint: disable=R0912, R0913, R0914, R0915, C0103, W0613

logger = logging.getLogger(__name__)


def status_start_(list_file_base: str, status_start_file: str) -> bool:
    start_file = os.path.join(list_file_base, status_start_file)
    dirs = os.path.dirname(start_file)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
    with open(start_file, "w") as w:
        w.write("Binary cif run started at %s." % str(datetime.datetime.now()))
    return True


def branching_(r: int) -> str:
    routes = ["local", "sfapi", "k8s"]
    route = routes[r]
    if route == "sfapi":
        return "sfapi_tasks"
    if route == "k8s":
        return "k8s_tasks"
    return "local_branch"


def make_dirs_(workflow_utility: WorkflowUtilities) -> bool:
    """mounted paths must be already made"""
    if not os.path.exists(workflow_utility.updateBase):
        os.makedirs(workflow_utility.updateBase, mode=0o777)
    for content_type in workflow_utility.contentTypeDir.values():
        path = os.path.join(workflow_utility.updateBase, content_type)
        if not os.path.exists(path):
            os.mkdir(path, mode=0o777)
    return True


def get_pdb_list_(
    workflow_utility: WorkflowUtilities,
    load_type: str,
    list_file_base: str,
    pdb_list_filename: str,
    result=None,
) -> bool:
    outfile = os.path.join(list_file_base, pdb_list_filename)
    if os.path.exists(outfile):
        return True
    # list[str]
    # 'pdb_id partial_path content_type'
    pdb_list = workflow_utility.get_pdb_list(load_type)
    with open(outfile, "wb") as w:
        pickle.dump(pdb_list, w)
    return True


def get_csm_list_(
    workflow_utility: WorkflowUtilities,
    load_type: str,
    list_file_base: str,
    csm_list_filename: str,
    result=None,
) -> bool:
    outfile = os.path.join(list_file_base, csm_list_filename)
    if os.path.exists(outfile):
        return True
    # list[str]
    # 'pdb_id partial_path content_type'
    csm_list = workflow_utility.get_comp_list(load_type)
    if not csm_list:
        return False
    with open(outfile, "wb") as w:
        pickle.dump(csm_list, w)
    return True


def make_task_list_from_remote_(
    list_file_base: str,
    pdb_list_filename: str,
    csm_list_filename: str,
    input_list_filename: str,
    maxfiles: int,
    workflow_utility: WorkflowUtilities,
    result=None,
) -> bool:
    # read pdb list
    pdb_list = None
    with open(os.path.join(list_file_base, pdb_list_filename), "rb") as r:
        pdb_list = pickle.load(r)
    if not pdb_list:
        logger.error("error reading pdb list")
        return False
    # read csm list
    csm_list = None
    csm_list_path = os.path.join(list_file_base, csm_list_filename)
    if os.path.exists(csm_list_path):
        with open(csm_list_path, "rb") as r:
            csm_list = pickle.load(r)
    if not csm_list:
        logger.error("error reading csm list")
    else:
        # join lists
        pdb_list.extend(csm_list)
    # trim list if testing
    nfiles = len(pdb_list)
    logger.info("found %d cif files", nfiles)
    if nfiles == 0:
        return False
    if 0 < maxfiles < nfiles:
        nfiles = maxfiles
        pdb_list = pdb_list[0:nfiles]
        logger.info("reading only %d files", nfiles)
    # save input list
    outfile = os.path.join(list_file_base, input_list_filename)
    with open(outfile, "wb") as w:
        pickle.dump(pdb_list, w)
    return True


def make_task_list_from_local_(
    local_data_path: str, list_file_base: str, input_list_filename: str, result=None
) -> bool:
    """
    requires cif files in source folder with no subdirs
    writes to target folder with no subdirs
    """
    # traverse local folder
    tasklist = glob.glob(os.path.join(local_data_path, "*.cif.gz"))
    nfiles = len(tasklist)
    if nfiles == 0:
        tasklist = glob.glob(os.path.join(local_data_path, "*.cif"))
        nfiles = len(tasklist)
    logger.info("found %d cif files", nfiles)
    with open(os.path.join(list_file_base, input_list_filename), "wb") as w:
        pickle.dump(tasklist, w)
    return True


def split_tasks_(
    list_file_base: str,
    input_list_filename: str,
    input_list_2d: str,
    maxfiles: int,
    subtasks: int,
    result=None,
) -> List[int]:
    # read task list
    tasklist = None
    with open(os.path.join(list_file_base, input_list_filename), "rb") as r:
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
    tasks = split_list(nfiles, subtasks, tasklist)
    # save full tasks file
    logger.info("get local tasks saving %d tasks to %s", len(tasks), input_list_2d)
    with open(os.path.join(list_file_base, input_list_2d), "wb") as w:
        pickle.dump(tasks, w)
    # return list of task indices
    tasks = list(range(0, len(tasks)))
    logger.info("returning %d tasks", len(tasks))
    return tasks


def split_list(nfiles: int, subtasks: int, tasklist: List[str]) -> List[List[str]]:
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


def local_task_map_(
    index: int,
    *,
    list_file_base: str = None,
    input_list_2d: str = None,
    temp_path: str = None,
    update_base: str = None,
    compress: bool = False,
    local_inputs_or_remote: str = None,
    batch_size: int = 1,
    pdbx_dict: str = None,
    ma_dict: str = None,
    rcsb_dict: str = None,
    workflow_utility: WorkflowUtilities = None
) -> bool:
    # read sublist
    infiles = None
    with open(os.path.join(list_file_base, input_list_2d), "rb") as r:
        allfiles = pickle.load(r)
        infiles = allfiles[index]
    if not infiles:
        logger.error("error - no infiles")
        return False
    logger.info("task map has %d infiles", len(infiles))

    # form dictionary object
    da = None
    paths = [pdbx_dict, ma_dict, rcsb_dict]
    try:
        adapter = IoAdapter(raiseExceptions=True)
        containers = []
        for path in paths:
            containers += adapter.readFile(inputFilePath=path)
        da = DictionaryApi(containerList=containers, consolidate=True)
    except Exception as e:
        logger.exception("failed to create dictionary api: %s", str(e))

    # traverse sublist and send each input file to converter
    if (batch_size is None) or not str(batch_size).isdigit():
        batch_size = 1
    batch_size = int(batch_size)
    if batch_size == 0:
        batch_size = multiprocessing.cpu_count()
    procs = []
    if batch_size == 1:
        # process one file at a time
        for line in infiles:
            args = (
                line,
                local_inputs_or_remote,
                update_base,
                compress,
                temp_path,
                workflow_utility,
                da,
            )
            single_task(*args)
    else:
        # process with file batching
        nfiles = len(infiles)
        tasks = split_list(nfiles, batch_size, infiles)
        for task in tasks:
            args = (
                task,
                local_inputs_or_remote,
                update_base,
                compress,
                temp_path,
                workflow_utility,
                da,
            )
            p = multiprocessing.Process(target=batch_task, args=args)
            procs.append(p)
        for p in procs:
            p.start()
        for p in procs:
            p.join()
        procs.clear()

    return True


def batch_task(
    tasks,
    local_inputs_or_remote,
    update_base,
    compress,
    temp_path,
    workflow_utility,
    da,
):
    for task in tasks:
        single_task(
            task,
            local_inputs_or_remote,
            update_base,
            compress,
            temp_path,
            workflow_utility,
            da,
        )


def single_task(
    line,
    local_inputs_or_remote,
    update_base,
    compress,
    temp_path,
    workflow_utility,
    da,
):
    """
    download to cif_file_path
    form output path bcif_file_path
    """
    if local_inputs_or_remote == "local":
        cif_file_path = line
        if (
            not os.path.exists(cif_file_path)
            and not os.path.exists(cif_file_path.replace(".gz", ""))
            and not os.path.exists("%s.gz" % cif_file_path)
        ):
            logger.error("error - could not find %s", cif_file_path)
            return
        pdb_file_name = os.path.basename(cif_file_path)
        bcif_file_path = os.path.join(
            update_base,
            pdb_file_name.replace(".cif.gz", ".bcif").replace(".cif", ".bcif"),
        )
        if compress:
            bcif_file_path = "%s.gz" % bcif_file_path
        logger.info("converting %s to %s", cif_file_path, bcif_file_path)
    else:
        tokens = line.split(" ")
        divided_path = tokens[1]
        enum_type = tokens[2]
        pdb_filename = os.path.basename(divided_path)
        cif_file_path = os.path.join(temp_path, pdb_filename)
        content_type = workflow_utility.contentTypeDir[enum_type]
        bcif_file_path = os.path.join(
            update_base,
            content_type,
            divided_path.replace(".cif.gz", ".bcif").replace(".cif", ".bcif"),
        )
        if compress:
            bcif_file_path = "%s.gz" % bcif_file_path
        url = workflow_utility.get_download_url(divided_path, enum_type)
        try:
            r = requests.get(url, timeout=300, stream=True)
            if r and r.status_code < 400:
                dirs = os.path.dirname(cif_file_path)
                if not os.path.exists(dirs):
                    os.makedirs(dirs, mode=0o777)
                    shutil.chown(dirs, "root", "root")
                with open(cif_file_path, "ab") as w:
                    for chunk in r.raw.stream(1024, decode_content=False):
                        if chunk:
                            w.write(chunk)
                shutil.chown(cif_file_path, "root", "root")
                os.chmod(cif_file_path, 0o777)
            else:
                raise requests.exceptions.RequestException(
                    "error - request failed for %s" % url
                )
        except Exception as e:
            logger.exception(str(e))
            if os.path.exists(cif_file_path):
                os.unlink(cif_file_path)
            return
    if os.path.exists(bcif_file_path):
        logger.info("file %s already exists", bcif_file_path)
        if local_inputs_or_remote == "remote":
            os.unlink(cif_file_path)
        return
    # make nested directories
    dirs = os.path.dirname(bcif_file_path)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
        shutil.chown(dirs, "root", "root")
        os.chmod(dirs, 0o777)
    # convert to bcif
    try:
        result = bcifconvert(cif_file_path, bcif_file_path, temp_path, da)
        if not result:
            raise Exception("failed to convert %s" % cif_file_path)
        shutil.chown(bcif_file_path, "root", "root")
        os.chmod(bcif_file_path, 0o777)
    except Exception as e:
        logger.exception(str(e))
    # remove input file
    finally:
        if local_inputs_or_remote == "remote":
            os.unlink(cif_file_path)


def validate_output_(
    *,
    list_file_base: str = None,
    input_list_filename: str = None,
    update_base: str = None,
    compress: bool = None,
    missing_file_base: str = None,
    missing_filename: str = None,
    workflow_utility: WorkflowUtilities = None,
    result=None
) -> bool:
    input_list_file = os.path.join(list_file_base, input_list_filename)
    if not os.path.exists(input_list_file):
        return False
    missing = []
    with open(input_list_file, "rb") as r:
        data = pickle.load(r)
        for line in data:
            divided_path = line.split()[1]
            content_type = workflow_utility.contentTypeDir[line.split()[2]]
            basename = os.path.join(update_base, content_type)
            filepath = os.path.join(basename, divided_path)
            out = filepath.replace(".cif.gz", ".bcif").replace(".cif", ".bcif")
            if compress:
                out = "%s.gz" % out
            if not os.path.exists(out):
                missing.append(out)
    if len(missing) > 0:
        missing_file = os.path.join(missing_file_base, missing_filename)
        with open(missing_file, "w") as w:
            for line in missing:
                w.write(line)
    return True


def remove_temp_files_(temp_path: str, list_file_base: str, result=None) -> bool:
    if not os.path.exists(temp_path):
        return False
    try:
        for filename in os.listdir(temp_path):
            path = os.path.join(temp_path, filename)
            if os.path.isfile(path):
                os.unlink(path)
        for filename in os.listdir(list_file_base):
            path = os.path.join(list_file_base, filename)
            if os.path.isfile(path):
                os.unlink(path)
    except Exception as e:
        logger.warning(str(e))
    return True


def tasks_done_(result=None) -> bool:
    logger.info("task maps completed")
    return True


def k8s_branch_() -> bool:
    logger.info("using k8s tasks")
    return True


def status_complete_(list_file_base: str, status_complete_file: str) -> bool:
    """
    must occur after end_task
    """
    complete_file = os.path.join(list_file_base, status_complete_file)
    dirs = os.path.dirname(complete_file)
    if not os.path.exists(dirs):
        os.makedirs(dirs, mode=0o777)
    with open(complete_file, "w") as w:
        w.write(
            "Binary cif run completed successfully at %s."
            % str(datetime.datetime.now())
        )
    return True
