
from rcsb.workflow.chem.ChemCompSearchIndexWorkflow import ChemCompSearchIndexWorkflow
import argparse
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
logger = logging.getLogger()


def main():
    """Build indices and configuration files required to support the chemical search service."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--cachePath", required=True, help="Cache path.")
    parser.add_argument("--ccUrlTarget", required=True, help="Chem comp url target.")
    parser.add_argument("--birdUrlTarget", required=True, help="Bird url target.")
    parser.add_argument("--channel", required=True, help="Channel.")
    parser.add_argument("--useCache", action="store_true", default=False, required=True, help="Should it use the cache.")
    parser.add_argument("--ccFileNamePrefix", required=True, help="Chem comp file name prefix.")
    parser.add_argument("--numProc", required=True, help="Number of procs for makeIndices.")
    parser.add_argument("--blHostName", required=True, help="Build locker host name.")
    parser.add_argument("--blUploadPathUnsynced", required=True, help="Build locker upload path unsynced.")
    parser.add_argument("--sftpUserName", required=True, help="SFTP username.")
    parser.add_argument("--sftpUserPassword", required=True, help="SFTP user password.")
    args = parser.parse_args()

    # Build chemical indices -
    logger.info("Using cache path %r", args.cachePath)
    logger.info("Using ccFileNamePrefix %s", args.ccFileNamePrefix)
    ccidxWf = ChemCompSearchIndexWorkflow(cachePath=args.cachePath, ccFileNamePrefix=args.ccFileNamePrefix)
    okI = ccidxWf.makeIndices(
        args.ccUrlTarget, args.birdUrlTarget, numProc=int(args.numProc), useCache=args.useCache
    )
    logger.info("Chemical search index build completed with status %r", okI)
    #
    okS = False
    if okI:
        # Store chemical indices for future use -
        sftpHost = args.blHostName
        okS = ccidxWf.stashIndices(
            "sftp://" + sftpHost,
            args.blUploadPathUnsynced,
            bundleLabel=args.channel.upper(),
            userName=args.sftpUserName,
            pw=args.sftpUserPassword,
        )
        logger.info("Chemical search index stashed with status %r", okS)
    else:
        msg = "Chemical search index completed with status %r" % okI
        raise ValueError(msg)
    #

    if not okS:
        error_msg = "Failed to stash chemical search index data"
        raise ValueError(error_msg)

    message = "Chemical Search Index completed with status %r stash status %r" % (okI, okS)
    logger.info("Completion message is %s", message)


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        fail_msg = f"Run failed {err}"
        raise RuntimeError(fail_msg) from err
