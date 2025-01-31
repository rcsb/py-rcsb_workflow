##
# File: ExDbExec.py
# Date: 22-Apr-2019  jdw
#
#  Execution wrapper  --  for extract and load operations -
#
#  Updates:
#   4-Sep-2019 jdw add Tree and Drugbank loaders
#  14-Feb-2020 jdw change over to ReferenceSequenceAnnotationProvider/Adapter
#   9-Mar-2023 dwp Lower refChunkSize to 10 (UniProt API having trouble streaming XML responses)
#  25-Apr-2024 dwp Add arguments and logic to support CLI usage from weekly-update workflow;
#                  Add support for logging output to a specific file
#  20-Aug-2024 dwp Add load_target_cofactors operation; change name of upd_targets_cofactors to upd_targets
#  22-Oct-2024 dwp Add ccd_img_gen and ccd_file_gen operations
#                  (latter will only be used briefly, as will stop generating SDF and Mol2 files in Dec 2024)
#  31-Jan-2025 mjt Moved this script from rcsb.exdb (to remove circluar dependencies)
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import os
import sys
import argparse
import logging

from rcsb.utils.config.ConfigUtil import ConfigUtil
from rcsb.workflow.wuw.ExDbWorkflow import ExDbWorkflow

HERE = os.path.abspath(os.path.dirname(__file__))
TOPDIR = os.path.dirname(os.path.dirname(os.path.dirname(HERE)))

# logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s", stream=sys.stdout)
logger = logging.getLogger()


def main():
    parser = argparse.ArgumentParser()
    #
    parser.add_argument(
        "--op",
        default=None,
        required=True,
        help="Loading operation to perform",
        choices=[
            "etl_chemref",  # ETL integrated chemical reference data
            "etl_uniprot_core",  # ETL UniProt core reference data
            "etl_tree_node_lists",  # ETL tree node lists
            "upd_ref_seq",  # Update reference sequence assignments
            "upd_neighbor_interactions",
            "upd_uniprot_taxonomy",
            "upd_targets",
            "load_target_cofactors",
            "upd_pubchem",
            "upd_entry_info",
            "upd_glycan_idx",
            "upd_resource_stash",
            "ccd_img_gen",
            "ccd_file_gen",
        ]
    )
    parser.add_argument(
        "--load_type",
        default="full",
        help="Type of load ('full' for complete and fresh single-worker load, 'replace' for incremental and multi-worker load)",
        choices=["full", "replace"],
    )
    #
    parser.add_argument("--config_path", default=None, help="Path to configuration options file")
    parser.add_argument("--config_name", default="site_info_remote_configuration", help="Configuration section name")
    parser.add_argument("--cache_path", default=None, help="Cache path for resource files")
    parser.add_argument("--num_proc", default=2, help="Number of processes to execute (default=2)")
    parser.add_argument("--chunk_size", default=10, help="Number of files loaded per process")
    parser.add_argument("--max_step_length", default=500, help="Maximum subList size (default=500)")
    parser.add_argument("--db_type", default="mongo", help="Database server type (default=mongo)")
    parser.add_argument("--document_limit", default=None, help="Load document limit for testing")
    #
    parser.add_argument("--rebuild_cache", default=False, action="store_true", help="Rebuild cached resource files")
    parser.add_argument("--rebuild_sequence_cache", default=False, action="store_true", help="Rebuild cached resource files for reference sequence updates")
    parser.add_argument("--provider_types_exclude", default=None, help="Resource provider types to exclude")
    parser.add_argument("--use_filtered_tax_list", default=False, action="store_true", help="Use filtered list for taxonomy tree loading")
    parser.add_argument("--disable_read_back_check", default=False, action="store_true", help="Disable read back check on all documents")
    parser.add_argument("--debug", default=False, action="store_true", help="Turn on verbose logging")
    parser.add_argument("--mock", default=False, action="store_true", help="Use MOCK repository configuration for testing")
    parser.add_argument("--log_file_path", default=None, help="Path to runtime log file output.")
    #
    # Arguments specific for op == 'upd_ref_seq'
    parser.add_argument("--ref_chunk_size", default=10, help="Max chunk size for reference sequence updates (for op 'upd_ref_seq')")
    parser.add_argument("--min_missing", default=0, help="Minimum number of allowed missing reference sequences (for op 'upd_ref_seq')")
    parser.add_argument("--min_match_primary_percent", default=None, help="Minimum reference sequence match percentage (for op 'upd_ref_seq')")
    parser.add_argument("--test_mode", default=False, action="store_true", help="Test mode for reference sequence updates (for op 'upd_ref_seq')")
    #
    # Arguments specific for op == 'ccd_img_gen' or 'ccd_file_gen'
    parser.add_argument("--cc_output_path", default=None, help="The base local directory path where chemical component files (image, coordinates) are written (for op 'ccd_img_gen')")
    parser.add_argument("--cc_cache_path", default=None, help="The base local directory path where chemical component cache data are written (for op 'ccd_img_gen')")
    parser.add_argument("--oe_license_path", default=None, help="Path to OpenEye license file")
    #
    # Arguments buildExdbResources
    parser.add_argument("--rebuild_all_neighbor_interactions", default=False, action="store_true", help="Rebuild all neighbor interactions from scratch (default is incrementally)")
    parser.add_argument("--cc_file_prefix", default="cc-full", help="File name discriminator for index sets")
    parser.add_argument("--cc_url_target", default=None, help="target url for chemical component dictionary resource file (default: None=all public)")
    parser.add_argument("--bird_url_target", default=None, help="target url for bird dictionary resource file (cc format) (default: None=all public)")
    #
    args = parser.parse_args()
    #
    try:
        op, commonD, loadD = processArguments(args)
    except Exception as err:
        logger.exception("Argument processing problem %s", str(err))
        raise ValueError("Argument processing problem") from err
    #
    #
    # Log input arguments
    loadLogD = {k: v for d in [commonD, loadD] for k, v in d.items() if k != "inputIdCodeList"}
    logger.info("running load op %r on loadLogD %r:", op, loadLogD)
    #
    # Run the operation
    okR = False
    exWf = ExDbWorkflow(**commonD)
    if op in ["etl_chemref", "etl_uniprot_core", "etl_tree_node_lists", "upd_ref_seq"]:
        okR = exWf.load(op, **loadD)
    elif op in [
        "upd_neighbor_interactions",
        "upd_uniprot_taxonomy",
        "upd_targets",
        "load_target_cofactors",
        "upd_pubchem",
        "upd_entry_info",
        "upd_glycan_idx",
        "upd_resource_stash",
    ]:
        okR = exWf.buildExdbResource(op, **loadD)
    elif op in [
        "ccd_img_gen",
        "ccd_file_gen",
    ]:
        okR = exWf.generateCcdFiles(op, **loadD)
    else:
        logger.error("Unsupported op %r", op)
    #
    logger.info("Operation %r completed with status %r", op, okR)
    #
    if not okR:
        logger.error("Operation %r failed with status %r", op, okR)
        raise ValueError("Operation %r failed" % op)


def processArguments(args):
    # Logging details
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
        formatter = logging.Formatter("%(asctime)s [%(levelname)s]-%(module)s.%(funcName)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    #
    # Configuration details
    configPath = args.config_path
    configName = args.config_name
    if not (configPath and configName):
        logger.error("Config path and/or name not provided: %r, %r", configPath, configName)
        raise ValueError("Config path and/or name not provided: %r, %r" % (configPath, configName))
    mockTopPath = os.path.join(TOPDIR, "rcsb", "mock-data") if args.mock else None
    logger.info("Using configuration file %r (section %r)", configPath, configName)
    cfgOb = ConfigUtil(configPath=configPath, defaultSectionName=configName, mockTopPath=mockTopPath)
    cfgObTmp = cfgOb.exportConfig()
    logger.info("Length of config object (%r)", len(cfgObTmp))
    if len(cfgObTmp) == 0:
        logger.error("Missing or access issue for config file %r", configPath)
        raise ValueError("Missing or access issue for config file %r" % configPath)
    else:
        del cfgObTmp
    #
    # Do any additional argument checking
    op = args.op
    if not op:
        raise ValueError("Must supply a value to '--op' argument")
    #
    cachePath = args.cache_path if args.cache_path else "."
    cachePath = os.path.abspath(cachePath)

    if args.db_type != "mongo":
        logger.error("Unsupported database type %r (must be 'mongo')", args.db_type)
        raise ValueError("Unsupported database type %r (must be 'mongo')" % args.db_type)

    # Now collect arguments into dictionaries
    commonD = {
        "configPath": configPath,
        "configName": configName,
        "cachePath": cachePath,
        "mockTopPath": mockTopPath,
        "debugFlag": debugFlag,
        "rebuildCache": args.rebuild_cache,
        "providerTypeExcludeL": args.provider_types_exclude,
    }
    loadD = {
        "loadType": args.load_type,
        "numProc": int(args.num_proc),
        "chunkSize": int(args.chunk_size),
        "maxStepLength": int(args.max_step_length),
        "dbType": args.db_type,
        "documentLimit": int(args.document_limit) if args.document_limit else None,
        "readBackCheck": not args.disable_read_back_check,
        "rebuildSequenceCache": args.rebuild_sequence_cache,
        "useFilteredLists": args.use_filtered_tax_list,
        "refChunkSize": int(args.ref_chunk_size),
        "minMissing": int(args.min_missing),
        "minMatchPrimaryPercent": float(args.min_match_primary_percent) if args.min_match_primary_percent else None,
        "testMode": args.test_mode,
        "rebuildAllNeighborInteractions": args.rebuild_all_neighbor_interactions,
        "ccFileNamePrefix": args.cc_file_prefix,
        "ccUrlTarget": args.cc_url_target,
        "birdUrlTarget": args.bird_url_target,
        "ccOutputPath": args.cc_output_path,
        "ccCachePath": args.cc_cache_path,
        "licenseFilePath": args.oe_license_path,
    }

    return op, commonD, loadD


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("Run failed %s", str(e))
        sys.exit(1)
