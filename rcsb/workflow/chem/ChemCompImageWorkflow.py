##
# File: ChemCompImageWorkflow.py
# Date: 10-Mar-2020  jdw
#
#  Workflow wrapper  --  chemical component image generation --
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

from rcsb.utils.chem.OeDepict import OeDepict
from rcsb.utils.chem.OeMoleculeProvider import OeMoleculeProvider

logger = logging.getLogger(__name__)


class ChemCompImageWorkflow(object):
    def __init__(self, **kwargs):
        """Module entry point for chemical component definition 2D image generation

        Args:
            ccUrlTarget (str): URL or path for concatenated chemical component dictionary (default: public wwPDB ftp)
            birdUrlTarget (str): URL or path for concatenated BIRD dictionary (default: public wwPDB ftp)
            licenseFilePath (str) = path to OpenEye license text file
            imagePath(str): directory containing generated image tree
            cachePath(str): cache directory for temporary files

        """
        #
        ccUrlTarget = kwargs.get("ccUrlTarget", None)
        birdUrlTarget = kwargs.get("birdUrlTarget", None)
        self.__licensePath = kwargs.get("licenseFilePath", "oe_license.txt")
        self.__imagePath = kwargs.get("imagePath", ".")
        cachePath = kwargs.get("cachePath", ".")
        cachePath = os.path.abspath(cachePath)

        #
        self.__oemp = OeMoleculeProvider(
            ccUrlTarget=ccUrlTarget,
            birdUrlTarget=birdUrlTarget,
            ccFileNamePrefix="cc-full",
            cachePath=cachePath,
            molBuildType="model-xyz",
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

    def makeImages(self):
        """Create images for all public chemical components with and without atom labels."""
        try:
            if not self.__setLicense(self.__licensePath):
                logger.error("Invalid license details - exiting")
                return False
            for ccId, oeMol in self.__oeMolD.items():
                imagePath = os.path.join(self.__imagePath, "image", ccId[0], ccId + ".svg")
                oed = OeDepict()
                title = ""
                oed.setMolTitleList([(ccId, oeMol, title)])
                # ---
                bondDisplayWidth = 10.0
                numAtoms = oeMol.NumAtoms()
                if numAtoms > 100 and numAtoms <= 200:
                    bondDisplayWidth = 6.0
                elif numAtoms > 200:
                    bondDisplayWidth = 4.0
                # ---
                oed.setDisplayOptions(
                    labelAtomName=False,
                    labelAtomCIPStereo=True,
                    labelAtomIndex=False,
                    labelBondIndex=False,
                    labelBondCIPStereo=True,
                    cellBorders=False,
                    bondDisplayWidth=bondDisplayWidth,
                )
                oed.setGridOptions(rows=1, cols=1, cellBorders=False)
                oed.prepare()
                oed.write(imagePath)
            for ccId, oeMol in self.__oeMolD.items():
                imagePath = os.path.join(self.__imagePath, "image_labeled", ccId[0], ccId + ".svg")
                oed = OeDepict()
                title = ""
                oed.setMolTitleList([(ccId, oeMol, title)])
                # ---
                bondDisplayWidth = 10.0
                numAtoms = oeMol.NumAtoms()
                if numAtoms > 100 and numAtoms <= 200:
                    bondDisplayWidth = 6.0
                elif numAtoms > 200:
                    bondDisplayWidth = 4.0
                # ---
                oed.setDisplayOptions(
                    labelAtomName=True,
                    labelAtomCIPStereo=True,
                    labelAtomIndex=False,
                    labelBondIndex=False,
                    labelBondCIPStereo=True,
                    cellBorders=False,
                    bondDisplayWidth=bondDisplayWidth,
                )
                oed.setGridOptions(rows=1, cols=1, cellBorders=False)
                oed.prepare()
                oed.write(imagePath)
            return True
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return False
