from rcsb.workflow.bcif.task_functions import (
    status_start_,
    make_dirs_,
    get_pdb_list_,
    get_csm_list_,
    make_task_list_from_remote_,
    make_task_list_from_local_,
    split_tasks_,
    local_task_map_,
    validate_output_,
    remove_temp_files_,
    tasks_done_,
    status_complete_,
)
from rcsb.workflow.bcif.workflow_functions import WorkflowUtilities

# pylint: disable=R0902, C0103


class BcifWorkflow:

    def __init__(self, args):

        self.nfiles = int(args.nfiles)
        self.coast = args.coast
        self.output_path = args.output_path
        self.temp_path = args.temp_path
        self.input_path = args.input_path
        self.subtasks = int(args.subtasks)
        self.batch_size = int(args.batch_size)
        self.local_inputs_or_remote = args.local_inputs_or_remote
        self.load_type = args.load_type
        self.list_file_base = args.list_file_base
        self.pdb_list_filename = args.pdb_list_filename
        self.csm_list_filename = args.csm_list_filename
        self.input_list_filename = args.input_list_filename
        self.input_list_2d = args.input_list_2d
        self.status_start_file = args.status_start_file
        self.status_complete_file = args.status_complete_file
        self.missing_file_base = args.missing_file_base
        self.missing_filename = args.missing_filename
        self.pdbx_dict = args.pdbx_dict
        self.ma_dict = args.ma_dict
        self.rcsb_dict = args.rcsb_dict
        self.prereleaseFtpFileBasePath = args.prereleaseFtpFileBasePath
        self.pdbIdsTimestampFilePath = args.pdbIdsTimestampFilePath
        self.csmFileRepoBasePath = args.csmFileRepoBasePath
        self.csmHoldingsUrl = args.csmHoldingsUrl
        self.structureFilePath = args.structureFilePath
        self.compress = bool(args.compress)
        self.interpolation = bool(args.interpolation)

    def log_exception(self, msg):
        raise RuntimeError("%s reporting %s" % (self.coast, msg))

    def __call__(self):

        if not status_start_(self.list_file_base, self.status_start_file):
            self.log_exception("status start failed")

        workflow_utility = WorkflowUtilities(
            coast=self.coast,
            interpolation=self.interpolation,
            out=self.output_path,
            prereleaseFtpFileBasePath=self.prereleaseFtpFileBasePath,
            pdbIdsTimestampFilePath=self.pdbIdsTimestampFilePath,
            csmFileRepoBasePath=self.csmFileRepoBasePath,
            csmHoldingsUrl=self.csmHoldingsUrl,
            structureFilePath=self.structureFilePath,
        )

        if not make_dirs_(workflow_utility):
            self.log_exception("make dirs failed")

        if self.local_inputs_or_remote == "remote":

            if (
                not get_pdb_list_(
                    workflow_utility,
                    self.load_type,
                    self.list_file_base,
                    self.pdb_list_filename,
                )
                or not get_csm_list_(
                    workflow_utility,
                    self.load_type,
                    self.list_file_base,
                    self.csm_list_filename,
                )
                or not make_task_list_from_remote_(
                    self.list_file_base,
                    self.pdb_list_filename,
                    self.csm_list_filename,
                    self.input_list_filename,
                    self.nfiles,
                    workflow_utility,
                )
            ):
                self.log_exception("make task list from remote failed")

        elif not make_task_list_from_local_(
            self.input_path, self.list_file_base, self.input_list_filename
        ):
            self.log_exception("make task list from local failed")

        if not split_tasks_(
            self.list_file_base,
            self.input_list_filename,
            self.input_list_2d,
            self.nfiles,
            self.subtasks,
        ):
            self.log_exception("split tasks failed")

        index = 0
        params = {
            "list_file_base": self.list_file_base,
            "input_list_2d": self.input_list_2d,
            "temp_path": self.temp_path,
            "update_base": self.output_path,
            "compress": self.compress,
            "local_inputs_or_remote": self.local_inputs_or_remote,
            "batch_size": self.batch_size,
            "pdbx_dict": self.pdbx_dict,
            "ma_dict": self.ma_dict,
            "rcsb_dict": self.rcsb_dict,
            "workflow_utility": workflow_utility,
        }
        if not local_task_map_(index, **params):
            self.log_exception("local task map failed")

        params = {
            "list_file_base": self.list_file_base,
            "input_list_filename": self.input_list_filename,
            "update_base": self.output_path,
            "compress": self.compress,
            "missing_file_base": self.missing_file_base,
            "missing_filename": self.missing_filename,
            "workflow_utility": workflow_utility,
        }
        if not validate_output_(**params):
            self.log_exception("validate output failed")

        if not remove_temp_files_(self.temp_path, self.list_file_base):
            self.log_exception("remove temp files failed")

        if not tasks_done_():
            self.log_exception("tasks done failed")

        if not status_complete_(self.list_file_base, self.status_complete_file):
            self.log_exception("status complete failed")
