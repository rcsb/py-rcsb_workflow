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
__author__ = "Michael Trumbull"
__email__ = "michael.trumbull@rcsb.org"
__license__ = "Apache 2.0"

import argparse
from rcsb.workflow.wuw.PdbCsmImageWorkflow import PdbCsmImageWorkflow


def main() -> None:
    """Cli parser for images workflow."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--op",
        required=True,
        help="which function to call",
        choices=[
            "genLists",
            "genJpgs",
        ],
    )

    parser.add_argument("--pdbBaseDir")
    parser.add_argument("--csmBaseDir")

    parser.add_argument("--idListPath")
    parser.add_argument("--idListName")
    parser.add_argument("--updateTmpBase")
    parser.add_argument("--imagesTmpBase")
    parser.add_argument("--molrenderExe", default="/opt/modules/node_modules/molrender/build/bin/molrender.js")
    parser.add_argument("--jpgsOutDir")

    parser.add_argument("--fileNumber")

    parser.add_argument("--jpgHeight", default=500)
    parser.add_argument("--jpgWidth", default=500)
    parser.add_argument("--jpgFormat", default='jpeg')
    parser.add_argument("--jpgAdditionalCmds", default=None)
    parser.add_argument("--jpgXvfbExecutable", default='/usr/bin/xvfb-run')
    parser.add_argument("--jpgScreen", default='1280x1024x24')
    parser.add_argument("--jpgRender", default='all')

    args = parser.parse_args()

    imgWF = PdbCsmImageWorkflow()
    if args.op == "genJpgs":
        imgWF.imagesGenJpgs(**args)
    else:
        raise ValueError("Cli --op flag error: not availible option")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        msg = f"Run failed {err}"
        raise RuntimeError(msg) from err
