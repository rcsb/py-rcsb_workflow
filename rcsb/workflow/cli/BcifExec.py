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
    subparsers = parser.add_subparsers(dest="mode", help="sub-command help")
    wuwparser = subparsers.add_parser(
        "wuw", aliases=["workflow"], help="run weekly update workflow"
    )
    convertparser = subparsers.add_parser("convert", help="convert cif to bcif")
    deconvertparser = subparsers.add_parser("deconvert", help="deconvert bcif to cif")

    # paths
    wuwparser.add_argument(
        "--listFileBase",
        default="/tmp",
        required=True,
        help="path for input lists",
    )
    wuwparser.add_argument(
        "--listFileName",
        default="pdbx_core_ids-1.txt",
        required=True,
        help="name of list file to read",
    )
    wuwparser.add_argument(
        "--remotePath",
        default="http://files.wwpdb.org/pub/pdb/data/structures/divided/mmCIF/",
        required=True,
        help="url or directory for cif files",
    )
    wuwparser.add_argument(
        "--outputPath",
        default="/mnt/vdb1/out",
        required=True,
        help="output directory for bcif files",
    )
    convertparser.add_argument(
        "--infile", required=True, help="optional infile for convert mode"
    )
    convertparser.add_argument(
        "--outfile",
        required=True,
        help="optional outfile for convert mode",
    )
    deconvertparser.add_argument(
        "--infile", required=True, help="optional infile for deconvert mode"
    )
    deconvertparser.add_argument(
        "--outfile",
        required=True,
        help="optional outfile for deconvert mode",
    )
    # settings
    wuwparser.add_argument(
        "--contentType",
        default="pdb",
        required=True,
        choices=["pdb", "csm", "ihm"],
        help="which type of experiment was performed",
    )
    wuwparser.add_argument(
        "--nfiles",
        default=0,
        required=False,
        help="set 0 for all files, set less than N for a test run, will not produce exactly n files",
    )
    wuwparser.add_argument(
        "--outfileSuffix",
        default=".bcif.gz",
        required=False,
        choices=[".bcif", ".bcif.gz"],
        help="whether to use additional gzip compression",
    )
    wuwparser.add_argument(
        "--batchSize",
        default=1,
        required=False,
        help="optional subdivisions of sublists for pdb list and csm list",
    )
    # output folder structure, default none (save all output files in one folder)
    wuwparser.add_argument(
        "--outputContentType",
        action="store_true",
        default=False,
        required=False,
        help="whether output paths should include a directory for the content type (pdb, csm)",
    )
    wuwparser.add_argument(
        "--outputHash",
        action="store_true",
        default=False,
        required=False,
        help="whether output paths should include the hash for the entry",
    )
    # input folder structure, default none (read all input files from one folder)
    wuwparser.add_argument(
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
    parser.add_argument(
        "--flrDict",
        default="https://raw.githubusercontent.com/ihmwg/flrCIF/refs/heads/master/dist/mmcif_ihm_flr_ext.dic",
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
