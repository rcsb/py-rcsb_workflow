"""Cli parser for jpg generation. This is a portion of imgs workflow which takes bcif images and generates these jpgs as well as chem images."""
##
# File: ImgExec.py
# Date: 11-Dec-2024  mjt
#
#  Execution wrapper  --  for jpg generation -
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

    parser.add_argument("--pdbBaseDir", help="Base path for experimental bcif file.")
    parser.add_argument("--csmBaseDir", help="Base path for computational bcif file.")
    parser.add_argument("--idListFilePath", help="List of ids to generate images for.")
    parser.add_argument("--molrenderExe", default="/opt/modules/node_modules/molrender/build/bin/molrender.js", help="Molrender executable location.")
    parser.add_argument("--jpgsOutDir", help="Where to put the complete jpg images.")
    parser.add_argument("--jpgHeight", default=500, help="Height of generated jpg.")
    parser.add_argument("--jpgWidth", default=500, help="Width of generated jpg.")
    parser.add_argument("--jpgFormat", default="jpeg", help="Output image format.")
    parser.add_argument("--jpgXvfbExecutable", default="/usr/bin/xvfb-run", help="Linux executable that allows this gui application to run in cli.")
    parser.add_argument("--jpgScreen", default="1280x1024x24", help="Screen dimensions image will be rendered for.")
    parser.add_argument("--jpgRender", default="all", help="Which elements of the potein do you want to render.")
    parser.add_argument("--checkFileAppend", default="_model-1.jpeg", help="What jpg file do you want to check exists after the process runs.")
    parser.add_argument("--contentTypeDir", default="pdb", help="Is this list for pdb structures or csm?")
    parser.add_argument("--numProcs", default=1, help="How many processors are available for parallel execution.")

    args = vars(parser.parse_args())

    imgWF = PdbCsmImageWorkflow()
    if args["op"] == "genJpgs":
        imgWF.imagesGenJpgs(**args)
    else:
        raise ValueError("CLI --op flag error: not available option %r" % args.op)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        msg = f"Run failed {err}"
        raise RuntimeError(msg) from err
