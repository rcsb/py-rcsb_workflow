##
# File:    BcifExec.py
# Author:  James Smith
# Date:    21-Feb-2025
##

"""
Entry point for bcif command line interface.
"""

__docformat__ = "google en"
__author__ = "James Smith"
__email__ = "james.smith@rcsb.org"
__license__ = "Apache 2.0"

import argparse
import os
import logging
import time
from rcsb.workflow.wuw.BcifWorkflow import BcifWorkflow

logger = logging.getLogger()


def main():

    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    # paths
    parser.add_argument(
        "--listFileBase",
        default="/tmp",
        required=True,
        help="path for input lists",
    )
    parser.add_argument(
        "--listFileName",
        default="pdbx_core_ids-1.txt",
        required=True,
        help="name of list file to read",
    )
    parser.add_argument(
        "--remotePath",
        default="http://prereleaseftp-external-east.rcsb.org/pdb/data/structures/divided/mmCIF/",
        required=True,
        help="url or directory for cif files",
    )
    parser.add_argument(
        "--outputPath",
        default="/mnt/vdb1/out",
        required=True,
        help="output directory for bcif files",
    )
    # settings
    parser.add_argument(
        "--contentType",
        default="pdb",
        required=True,
        choices=["pdb", "csm", "ihm"],
        help="which type of experiment was performed",
    )
    parser.add_argument(
        "--nfiles",
        default=0,
        required=False,
        help="set 0 for all files, set less than N for a test run, will not produce exactly n files",
    )
    parser.add_argument(
        "--outfileSuffix",
        default=".bcif.gz",
        required=False,
        choices=[".bcif", ".bcif.gz"],
        help="whether to use additional gzip compression",
    )
    parser.add_argument(
        "--batchSize",
        default=1,
        required=False,
        help="optional subdivisions of sublists for pdb list and csm list",
    )
    # output folder structure, default none (save all output files in one folder)
    parser.add_argument(
        "--outputContentType",
        action="store_true",
        default=False,
        required=False,
        help="whether output paths should include a directory for the content type (pdb, csm)",
    )
    parser.add_argument(
        "--outputHash",
        action="store_true",
        default=False,
        required=False,
        help="whether output paths should include the hash for the entry",
    )
    # input folder structure, default none (read all input files from one folder)
    parser.add_argument(
        "--inputHash",
        action="store_true",
        default=False,
        required=False,
        help="whether local input paths should include the hash for the entry",
    )
    # config
    parser.add_argument(
        "--pdbxDict",
        default="https://raw.githubusercontent.com/wwpdb-dictionaries/mmcif_pdbx/master/dist/mmcif_pdbx_v5_next.dic",
        required=False,
    )
    parser.add_argument(
        "--maDict",
        default="https://raw.githubusercontent.com/ihmwg/ModelCIF/master/dist/mmcif_ma_ext.dic",
        required=False,
    )
    parser.add_argument(
        "--rcsbDict",
        default="https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/master/dictionary_files/dist/rcsb_mmcif_ext.dic",
        required=False,
    )
    parser.add_argument(
        "--ihmDict",
        default="https://raw.githubusercontent.com/rcsb/py-rcsb_exdb_assets/master/dictionary_files/reference/mmcif_ihm_ext.dic",
        required=False,
    )
    # logging
    parser.add_argument("--log_file_path", required=False)
    parser.add_argument("--debug", action="store_true", default=False, required=False)

    args = parser.parse_args()

    logFilePath = args.log_file_path
    debugFlag = args.debug
    if debugFlag:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if logFilePath:
        logDir = os.path.dirname(logFilePath)
        if not os.path.isdir(logDir):
            os.makedirs(logDir)
        handler = logging.FileHandler(logFilePath, mode="a")
        if debugFlag:
            handler.setLevel(logging.DEBUG)
        else:
            handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt="%(asctime)s @%(process)s [%(levelname)s]-%(module)s: %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    else:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            fmt="%(asctime)s @%(process)s [%(levelname)s]-%(module)s: %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.info("bcif workflow initialized")

    try:
        (BcifWorkflow(args))()
    except RuntimeError as e:
        raise Exception(str(e)) from e


if __name__ == "__main__":
    t = time.time()
    try:
        main()
    except Exception as e:
        logger.exception(str(e))
        raise e
    t2 = time.time() - t
    logger.info("completed in %.2f s", t2)
