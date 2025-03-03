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
import logging
import time
from rcsb.workflow.wuw.BcifWorkflow import BcifWorkflow

logger = logging.getLogger(__name__)


def main():

    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parser.add_subparsers(help="subcommand help")
    splitParser = subparsers.add_parser("split", help="split list files")
    splitParser.add_argument(
        "--subtasks",
        default=1,
        required=False,
        help="sublists for pdb list and csm list, if 0 will equal number of cpus",
    )
    splitParser.add_argument(
        "--loadType",
        default="incremental",
        choices=["full", "incremental"],
        required=False,
    )
    computeParser = subparsers.add_parser("compute", help="distribute computation")
    computeParser.add_argument(
        "--batch",
        default=0,
        required=False,
        help="subdivisions of sublists with same rules as subtasks",
    )
    computeParser.add_argument(
        "--maxTempFiles",
        default=100,
        required=False,
        help="max reads before clear out temp files",
    )
    computeParser.add_argument(
        "--nfiles",
        default=0,
        required=True,
        help="set 0 for all files, set less than N for a test run, will not produce exactly n files",
    )
    # paths
    parser.add_argument(
        "--outputPath",
        default="/mnt/vdb1/out",
        required=True,
        help="output directory for bcif files",
    )
    parser.add_argument(
        "--tempPath", default="/tmp", required=True, help="files are wiped on cleanup"
    )
    # not required (rely on defaults)
    parser.add_argument(
        "--outfileSuffix",
        default=".bcif.gz",
        required=False,
        choices=[".bcif", ".bcif.gz"],
        help="whether to use additional gzip compression",
    )
    parser.add_argument(
        "--inputPath",
        default="/mnt/vdb1/in",
        required=False,
        help="local workflow only",
    )
    parser.add_argument(
        "--listFileBase",
        default="/tmp",
        required=False,
        help="input lists, may be same as temp path",
    )
    parser.add_argument(
        "--localInputsOrRemote",
        default="remote",
        choices=["local", "remote"],
        required=False,
    )
    parser.add_argument("--statusStartFile", default="status.start", required=False)
    parser.add_argument(
        "--statusCompleteFile", default="status.complete", required=False
    )
    parser.add_argument("--missingFileBase", default="/home/ubuntu", required=False)
    parser.add_argument("--missingFileName", default="missing.txt", required=False)
    parser.add_argument(
        "--removedFileName",
        default="removed.txt",
        required=False,
        help="saved to missingFileBase",
    )
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
    # from sandbox_config.py/MasterConfig
    parser.add_argument(
        "--prereleaseFtpFileBasePath",
        default="http://prereleaseftp-external-east.rcsb.org/pdb",
        required=False,
    )
    parser.add_argument(
        "--pdbIdsTimestampFilePath",
        default="holdings/released_structures_last_modified_dates.json.gz",
        required=False,
    )
    parser.add_argument(
        "--csmFileRepoBasePath",
        default="http://computed-models-external-east.rcsb.org/staging",
        required=False,
    )
    parser.add_argument(
        "--compModelFileHoldingsList",
        default="holdings/computed-models-holdings-list.json",
        required=False,
        help="list file rather than holdings file itself",
    )
    parser.add_argument(
        "--structureFilePath", default="data/structures/divided/mmCIF/", required=False
    )
    parser.add_argument(
        "--configPath",
        default="./rcsb/workflow/bcif/config.yml",
        required=False,
        help="required for split id list",
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

    args = parser.parse_args()

    try:
        (BcifWorkflow(args))()
    except RuntimeError as e:
        raise Exception(str(e)) from e
    except ValueError as e:
        raise Exception(str(e)) from e
    except Exception as e:
        raise Exception(str(e)) from e


if __name__ == "__main__":
    t = time.time()
    try:
        main()
    except Exception as e:
        logger.exception(str(e))
    t2 = time.time() - t
    logger.info("completed in %.2f s", t2)
