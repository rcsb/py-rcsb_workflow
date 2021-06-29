##
# File: ChemCompIndexWorkflow.py
# Date: 2-Jun-2020  jdw
#
#  Workflow wrapper  --  generate chemical component and BIRD search indices  --
#
#  Updates:
#  10-Jun-2020 jdw Hookup to ChemCompSearchWrapper()
##
__docformat__ = "google en"
__author__ = "John Westbrook"
__email__ = "jwest@rcsb.rutgers.edu"
__license__ = "Apache 2.0"

import logging
import os

from rcsb.utils.chem.ChemCompSearchWrapper import ChemCompSearchWrapper

logger = logging.getLogger(__name__)


class ChemCompSearchIndexWorkflow(object):
    def __init__(self, **kwargs):
        """Module entry point for chemical component and BIRD search index generation workflow.

        Args:
            cachePath(str, optional): cache directory to store indices and temporary files (default: '.')
            licenseFilePath (str, optional) = path to OpenEye license text file (default: use enviroment OE_LICENSE setting)
            ccFilNamePrefix (str, optional) =  index prefix (default: "cc-full")
        """
        self.__licensePath = kwargs.get("licenseFilePath", "oe_license.txt")
        cachePath = kwargs.get("cachePath", ".")
        self.__cachePath = os.path.abspath(cachePath)
        self.__ccFileNamePrefix = kwargs.get("ccFilNamePrefix", "cc-full")
        self.__ccsw = ChemCompSearchWrapper(cachePath=self.__cachePath, ccFileNamePrefix=self.__ccFileNamePrefix)

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
            logger.error("Setting Openeye license file %r failing %s", licensePath, str(e))
        return ok

    def testCache(self):
        return True

    def makeIndices(self, ccUrlTarget, birdUrlTarget, **kwargs):
        """Build chemical component and BIRD search indices.

        Args:
            ccUrlTarget (str): URL or path for concatenated chemical component dictionary
            birdUrlTarget (str): URL or path for concatenated BIRD dictionary

            Other arguments may be supplied to change defaults for index generators (testing/troubleshooting)

        Returns:
            bool:  True for success or False otherwise

        """
        ok = False
        if not self.__setLicense(self.__licensePath):
            logger.error("Invalid OpenEye license details - exiting")
            return ok
        #
        try:
            ok = self.__ccsw.buildDependenices(ccUrlTarget, birdUrlTarget, **kwargs)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok

    def stashIndices(self, url, dirPath, bundleLabel="A", userName=None, pw=None):
        """Store a copy of the bundled search dependencies remotely -

        Args:
            url (str): URL string for the destination host (e.g. sftp://myserver.net or None for a local file)
            dirPath (str): directory path on the remote resource
            bundleLabel (str, optional): optional label preppended to the stashed dependency bundle artifact (default='A')
            userName (str, optional): optional access information. Defaults to None.
            password (str, optional): optional access information. Defaults to None.

        Returns:
          bool:  True for success or False otherwise

        """
        #
        ok = False
        try:
            ok = self.__ccsw.stashDependencies(url, dirPath, bundleLabel=bundleLabel, userName=userName, pw=pw)
        except Exception as e:
            logger.exception("Failing with %s", str(e))
        return ok
