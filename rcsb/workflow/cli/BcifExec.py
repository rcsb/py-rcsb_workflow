import os
import sys
import argparse
import logging
from rcsb.workflow.wuw.BcifWorkflow import *

logging.basicConfig(level=logging.INFO)

def main():

    parser = argparse.ArgumentParser(description="", formatter_class=argparse.RawTextHelpFormatter)
    # settings
    parser.add_argument("--nfiles", default=0, required=True)
    parser.add_argument("--lang", default="python", choices=["python", "molstar", "java"], required=True)
    parser.add_argument("--coast", default="west", choices=["east", "west"], required=True)
    # paths
    parser.add_argument("--input_path", default="/mnt/vdb1/in", required=True)
    parser.add_argument("--output_path", default="/mnt/vdb1/out", required=True)
    # not required (rely on defaults)
    parser.add_argument("--subtasks", default=1, required=False)
    parser.add_argument("--batch_size", default=0, required=False)
    parser.add_argument("--local_inputs_or_remote", default="remote", choices=["local", "remote"], required=False)
    parser.add_argument("--load_type", default="incremental", choices=["full", "incremental"], required=False)
    parser.add_argument("--interpolation", action="store_true", default=True, required=False)
    parser.add_argument("--list_file_base", default="/tmp", required=False)
    parser.add_argument("--pdb_list_filename", default="pdb_list.pkl", required=False)
    parser.add_argument("--csm_list_filename", default="csm_list.pkl", required=False)
    parser.add_argument("--input_list_filename", default="inputs.pkl", required=False)
    parser.add_argument("--input_list_2d", default="inputs2d.pkl", required=False)
    parser.add_argument("--status_start_file", default="status.start", required=False)
    parser.add_argument("--status_complete_file", default="status.complete", required=False)
    parser.add_argument("--molstar_cmd", default="lib/commonjs/servers/model/preprocess", required=False)
    parser.add_argument("--pdbx_dict", default="https://raw.githubusercontent.com/wwpdb-dictionaries/mmcif_pdbx/master/dist/mmcif_pdbx_v5_next.dic", required=False)
    parser.add_argument("--ma_dict", default="https://raw.githubusercontent.com/ihmwg/ModelCIF/master/dist/mmcif_ma_ext.dic", required=False)
    parser.add_argument("--rcsb_dict", default="https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/master/dictionary_files/dist/rcsb_mmcif_ext.dic", required=False)
    # from sandbox_config.py/MasterConfig
    parser.add_argument("--prereleaseFtpFileBasePath", default="http://prereleaseftp-external-%s.rcsb.org/pdb", required=False)
    parser.add_argument("--pdbIdsTimestampFilePath", default="holdings/released_structures_last_modified_dates.json.gz", required=False)
    parser.add_argument("--csmFileRepoBasePath", default="http://computed-models-external-%s.rcsb.org/staging", required=False)
    parser.add_argument("--csmHoldingsUrl", default="holdings/computed-models-holdings.json.gz", required=False)
    parser.add_argument("--structureFilePath", default="data/structures/divided/mmCIF/", required=False)

    args = parser.parse_args()

    (BcifWorkflow(args))()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(str(e))

