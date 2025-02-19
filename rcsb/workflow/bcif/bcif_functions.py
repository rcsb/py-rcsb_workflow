import logging
from mmcif.api.DictionaryApi import DictionaryApi
from rcsb.utils.io.MarshalUtil import MarshalUtil

logger = logging.getLogger(__name__)


def bcifconvert(infile: str, outfile: str, workpath: str, da: DictionaryApi) -> bool:
    mu = MarshalUtil(workPath=workpath)
    data = mu.doImport(infile, fmt="mmcif")
    try:
        result = mu.doExport(outfile, data, fmt="bcif", dictionaryApi=da)
        if not result:
            raise Exception()
    except Exception as e:
        logger.exception("error during bcif conversion: %s", str(e))
        return False
    return True
