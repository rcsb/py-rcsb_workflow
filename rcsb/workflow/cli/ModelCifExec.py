##
# File:    ModelCifExec.py
# Author:  Jose Duarte
# Date:    9-Oct-2026
##

"""
Entry point for converting structure predictions to ModelCIF BinaryCIF files.
"""

__docformat__ = "google en"
__author__ = "Jose Duarte"
__email__ = "jose.duarte@rcsb.org"
__license__ = "Apache 2.0"

import argparse
import logging

from rcsb.workflow.modelcif.OpenFold3ModelCif import convertOpenFold3Outputs, findQueryDirs

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


def main():
    """Convert the top-ranked sample of each query of an OpenFold3 run to ModelCIF (model and PAE BinaryCIF files)."""
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("--of3OutputDir", required=True, help="OpenFold3 output directory; query directories (<query>/seed_*) are found at any depth")
    parser.add_argument("--outputDir", required=True, help="Directory for the <query>_model.bcif and <query>_pae.bcif files")
    parser.add_argument("--of3Version", default="0.5", help="OpenFold3 version, recorded in the ModelCIF files")
    parser.add_argument("--checkpoint", default="of3-ob-2025-06-30-174k.pt", help="OpenFold3 checkpoint, recorded in the ModelCIF files")
    args = parser.parse_args()

    logger.info("Converting OpenFold3 outputs in %s to ModelCIF in %s", args.of3OutputDir, args.outputDir)
    if not findQueryDirs(args.of3OutputDir):
        msg = "No OpenFold3 query directories (<query>/seed_*) found under %s" % args.of3OutputDir
        raise ValueError(msg)
    failed = convertOpenFold3Outputs(args.of3OutputDir, args.outputDir, args.of3Version, args.checkpoint)
    if failed:
        msg = "ModelCIF conversion failed for %d queries: %s" % (len(failed), failed)
        raise ValueError(msg)
    logger.info("ModelCIF conversion completed")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        fail_msg = f"Run failed {err}"
        raise RuntimeError(fail_msg) from err
