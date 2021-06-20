##
# File: ChemCompFileWorkflow.py
# Date: 10-Mar-2020  jdw
#
#  Workflow wrapper  --  chemical component file conversion generator --
#
#  Updates:
#
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os

from rcsb.utils.chem.OeIoUtils import OeIoUtils
from rcsb.utils.chem.OeMoleculeProvider import OeMoleculeProvider

logger = logging.getLogger(__name__)


class ChemCompFileWorkflow(object):
    def __init__(self, **kwargs):
        """Module entry point for chemical component definition 2D image generation

        Args:
            ccUrlTarget (str): URL or path for concatenated chemical component dictionary (default: public wwPDB ftp)
            birdUrlTarget (str): URL or path for concatenated BIRD dictionary (default: public wwPDB ftp)
            licenseFilePath (str) = path to OpenEye license text file
            fileDirPath(str): directory containing generated image tree
            cachePath(str): cache directory for temporary files
            molBuildType(str): build type for constructing OE moleclues ('ideal-xyz', 'model-xyz' default: 'ideal-xyz')
        """
        #
        ccUrlTarget = kwargs.get("ccUrlTarget", None)
        birdUrlTarget = kwargs.get("birdUrlTarget", None)
        self.__licensePath = kwargs.get("licenseFilePath", "oe_license.txt")
        self.__fileDirPath = kwargs.get("fileDirPath", ".")
        self.__molBuildType = kwargs.get("molBuildType", "ideal-xyz")
        cachePath = kwargs.get("cachePath", ".")
        cachePath = os.path.abspath(cachePath)

        #
        self.__oemp = OeMoleculeProvider(
            ccUrlTarget=ccUrlTarget,
            birdUrlTarget=birdUrlTarget,
            ccFileNamePrefix="cc-full",
            cachePath=cachePath,
            molBuildType=self.__molBuildType,
            useCache=True,
            oeFileNamePrefix="oe-full",
        )
        self.__oeMolD = self.__oemp.getOeMolD()

    def __setLicense(self, licensePath):
        ok = False
        try:
            if os.environ.get("OE_LICENSE") and os.access(os.environ["OE_LICENSE"], os.R_OK):
                logger.info("Using license from environment %r", os.environ["OE_LICENSE"])
                ok = True
            elif os.access(licensePath, os.R_OK):
                os.environ["OE_LICENSE"] = licensePath
                logger.info("Setting environmenal variable OE_LICENSE to %r", os.environ["OE_LICENSE"])
                ok = True
        except Exception as e:
            logger.error("Setting license file %r failing %s", licensePath, str(e))
        return ok

    def testCache(self):
        return self.__oemp.testCache() if self.__oemp else False

    def makeFiles(self, fmt="sdf"):
        """Create files (mol, mol2) for all public chemical components."""
        try:

            if fmt not in ["mol", "mol2", "mol2h", "sdf"]:
                return False
            if not self.__setLicense(self.__licensePath):
                logger.error("Invalid license details - exiting")
                return False
            for ccId, oeMol in self.__oeMolD.items():
                if self.__molBuildType == "ideal-xyz":
                    filePath = os.path.join(self.__fileDirPath, fmt, ccId[0], ccId + "_ideal." + fmt)
                    oeioU = OeIoUtils()
                    oeioU.write(filePath, oeMol, constantMol=True)
                else:
                    filePath = os.path.join(self.__fileDirPath, fmt, ccId[0], ccId + "_model." + fmt)
                    oeioU = OeIoUtils()
                    oeioU.write(filePath, oeMol, constantMol=True)

            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False
