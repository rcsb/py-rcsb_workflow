import os
import sys
from rcsb.workflow.bcif.task_functions import *
from rcsb.workflow.bcif.workflow_functions import *
from rcsb.workflow.bcif.bcif_functions import *

class BcifWorkflow:

  def __init__(self, args):

    self.nfiles = int(args.nfiles)
    self.lang = args.lang
    self.coast = args.coast
    self.input_path = args.input_path
    self.output_path = args.output_path

    self.subtasks = int(args.subtasks)
    self.batch_size = int(args.batch_size)
    self.local_inputs_or_remote = args.local_inputs_or_remote
    self.load_type = args.load_type
    self.interpolation = bool(args.interpolation)
    self.list_file_base = args.list_file_base
    self.pdb_list_filename = args.pdb_list_filename
    self.csm_list_filename = args.csm_list_filename
    self.input_list_filename = args.input_list_filename
    self.input_list_2d = args.input_list_2d
    self.status_start_file = args.status_start_file
    self.status_complete_file = args.status_complete_file
    self.molstar_cmd = args.molstar_cmd
    self.pdbx_dict = args.pdbx_dict
    self.ma_dict = args.ma_dict
    self.rcsb_dict = args.rcsb_dict
    self.prereleaseFtpFileBasePath = args.prereleaseFtpFileBasePath
    self.pdbIdsTimestampFilePath = args.pdbIdsTimestampFilePath
    self.csmFileRepoBasePath = args.csmFileRepoBasePath
    self.csmHoldingsUrl = args.csmHoldingsUrl
    self.structureFilePath = args.structureFilePath

  def __call__(self):

    if not status_start_(self.list_file_base, self.status_start_file):
        raise RuntimeError('status start failed')

    workflow_utility = WorkflowUtilities(coast=self.coast, interpolation=self.interpolation, out=self.output_path, 
                       prereleaseFtpFileBasePath=self.prereleaseFtpFileBasePath,
                       pdbIdsTimestampFilePath=self.pdbIdsTimestampFilePath,
                       csmFileRepoBasePath=self.csmFileRepoBasePath,
                       csmHoldingsUrl=self.csmHoldingsUrl,
                       structureFilePath=self.structureFilePath)

    result1 = make_dirs_(workflow_utility)
    if not result1:
        raise RuntimeError('make dirs failed')

    if self.local_inputs_or_remote == "remote":
       result2 = get_pdb_list_(workflow_utility, self.load_type, self.list_file_base, self.pdb_list_filename, result1)
       if not result2:
           raise RuntimeError('get pdb list failed')
       result3 = get_csm_list_(workflow_utility, self.load_type, self.list_file_base, self.csm_list_filename, result2)
       if not result3:
           raise RuntimeError('get csm list failed')
       result4 = make_task_list_from_remote_(self.input_path, self.list_file_base, self.pdb_list_filename, self.csm_list_filename,
                                             self.input_list_filename, self.nfiles, workflow_utility, result3)
       if not result4:
           raise RuntimeError('make task list from remote failed')
    else:
       result4 = make_task_list_from_local_(self.input_path, self.list_file_base, self.input_list_filename, result1)
       if not result4:
           raise RuntimeError('make task list from local failed')

    if not split_tasks_(self.list_file_base, self.input_list_filename, self.input_list_2d, self.nfiles, self.subtasks, result4):
        raise RuntimeError('split tasks failed')

    index = 0
    if not local_task_map_(index, self.list_file_base, self.input_list_2d, self.input_path, self.output_path, self.local_inputs_or_remote, self.lang, self.batch_size, self.pdbx_dict, self.ma_dict, self.rcsb_dict, self.molstar_cmd, workflow_utility):
        raise RuntimeError('local task map failed')

    if not tasks_done_([]):
        raise RuntimeError('tasks done failed')

    if not status_complete_(self.list_file_base, self.status_complete_file):
        raise RuntimeError('status complete failed')

