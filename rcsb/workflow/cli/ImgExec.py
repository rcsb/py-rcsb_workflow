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
            "genJpgs",
        ],
    )

    parser.add_argument("--baseDir", help="Base path for experimental or computational bcif file.")
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
    parser.add_argument("--numProcs", default=1, help="How many processors are available for parallel execution.")
    parser.add_argument("--holdingsFilePath", help="Path (including filename) to holdings file .json file (csm, pdb, ihm)")
    parser.add_argument("--targetFileSuffix", default="_model-1.jpeg", help="string that follows the ID in the jpg file name for comparing timestamps.")
    parser.add_argument("--csmHoldingsFileSubstring", default="computed-models-holdings-list", help="substring in csm holdings file path to determine if we are working with CSMs.")
    parser.add_argument("--baseUrl", help="Base url for model file. This will supercede any local bcif.")
    parser.add_argument("--modelFileExtension", default=".bcif.gz", help="Model file type (.bcif.gz, .bcif, .cif).")
    parser.add_argument("--tmpDir", default="/tmp", help="Temporary dir for downloading model file over url.")
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
