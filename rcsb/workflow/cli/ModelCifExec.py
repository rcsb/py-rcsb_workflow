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
import inspect
import logging

from rcsb.workflow.modelcif.OpenFold3ModelCif import convertOpenFold3Outputs, findQueryDirs

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


def main():
    """Convert the best samples of each query of an OpenFold3 run to ModelCIF BinaryCIF (model file and optional PAE file).

    PAE is by far the largest part of the output: one row per residue pair, per model.
    --paeStep rounds it (0.5 Angstrom by default) and --gzip (on by default) is what turns
    that into smaller files: BinaryCIF takes about 2 bytes per PAE row at any step, but
    gzip compresses the coarser values much better. For a 1010-residue prediction the PAE
    file is 2.1 MB uncompressed at any step; gzipped, 1.1 MB at 0.01 A and 0.34 MB at 0.5 A.
    Only use --no-gzip if the files are compressed elsewhere.
    """
    parser = argparse.ArgumentParser(description=inspect.cleandoc(main.__doc__), formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--of3OutputDir", required=True, help="OpenFold3 output directory; query directories (<query>/seed_*) are found at any depth")
    parser.add_argument("--outputDir", required=True, help="Directory for the <query>_model.bcif.gz and <query>_pae.bcif.gz files")
    parser.add_argument("--of3Version", default="0.5", help="OpenFold3 version, recorded in the ModelCIF files")
    parser.add_argument("--checkpoint", default="of3-ob-2025-06-30-174k.pt", help="OpenFold3 checkpoint, recorded in the ModelCIF files")
    parser.add_argument("--numModels", type=int, default=1, help="Samples per query to write as ranked models, best first; 0 for all (default: 1)")
    parser.add_argument(
        "--rankBy", default="sample_ranking_score", help="OpenFold3 aggregated confidence score to rank samples by, e.g. avg_plddt or ptm (default: sample_ranking_score)"
    )
    parser.add_argument("--rankOrder", choices=["desc", "asc"], default="desc", help="desc if higher scores are better (default), asc if lower")
    parser.add_argument("--skipPae", action="store_true", default=False, help="Write only the model file, without the associated PAE file")
    parser.add_argument(
        "--paeStep",
        type=float,
        default=0.5,
        help="Round PAE values to multiples of this step, in Angstrom (default: 0.5, OpenFold3's PAE bin width). "
        "PAE is by far the largest part of the output; coarser steps make it compress much better with --gzip. 0.01 keeps OpenFold3's precision",
    )
    parser.add_argument(
        "--gzip",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Gzip the output files as .bcif.gz (default). Without gzip, a coarser --paeStep barely reduces the PAE file size",
    )
    args = parser.parse_args()

    logger.info("Converting OpenFold3 outputs in %s to ModelCIF in %s", args.of3OutputDir, args.outputDir)
    if not findQueryDirs(args.of3OutputDir):
        msg = "No OpenFold3 query directories (<query>/seed_*) found under %s" % args.of3OutputDir
        raise ValueError(msg)
    if args.numModels < 0:
        msg = "--numModels must be 0 (all) or positive, got %d" % args.numModels
        raise ValueError(msg)
    if args.paeStep <= 0:
        msg = "--paeStep must be positive, got %g" % args.paeStep
        raise ValueError(msg)
    failed = convertOpenFold3Outputs(
        args.of3OutputDir,
        args.outputDir,
        args.of3Version,
        args.checkpoint,
        numModels=args.numModels,
        rankBy=args.rankBy,
        descending=args.rankOrder == "desc",
        withPae=not args.skipPae,
        paeStep=args.paeStep,
        gzipped=args.gzip,
    )
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
