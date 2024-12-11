"""Cli parser for imgs workflow."""
##
# File: ImgExec.py
# Date: 11-Dec-2024  mjt
#
#  Execution wrapper  --  for chem image generation -
#
#  Updates:
#  
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import argparse
from rcsb.workflow.wuw.PdbCsmImageWorkflow import images_gen_jpgs, images_gen_lists


def main() -> None:
    """Cli parser for images workflow."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--op",
        required=True,
        help="which function to call",
        choices=[
            "gen_lists",
            "gen_jpgs",
        ],
    )

    parser.add_argument("--pdb_gz_path")
    parser.add_argument("--csm_gz_path")
    parser.add_argument("--pdb_base_dir")
    parser.add_argument("--csm_base_dir")
    parser.add_argument("--update_all_images", action='store_true')
    parser.add_argument("--imgs_exclude_models", action='store_true')
    parser.add_argument("--num_workers")

    parser.add_argument("--id_list_path")
    parser.add_argument("--update_tmp_base")
    parser.add_argument("--prerelease_ftp_file_base_path")
    parser.add_argument("--csm_file_repo_base_path")
    parser.add_argument("--bcif_exe")
    parser.add_argument("--images_tmp_base")
    parser.add_argument("--molrender_exe")
    parser.add_argument("--jpgs_out_dir")

    parser.add_argument("--file_number")

    args = vars(parser.parse_args())

    if args["op"] == "gen_lists":
        images_gen_lists(args)
    elif args["op"] == "gen_jpgs":
        images_gen_jpgs(args)
    else:
        msg = "Cli --op flag error: not availible option"
        raise ValueError(msg)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        msg = f"Run failed {err}"
        raise RuntimeError(msg) from err
