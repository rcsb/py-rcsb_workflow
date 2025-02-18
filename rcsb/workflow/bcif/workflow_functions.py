import urllib
import urllib.request
import urllib.parse
import urllib.error
import json
from enum import Enum
import gzip
import datetime
import os
import logging
from typing import Tuple, Dict

# pylint: disable=C0103, R0902, R0913, R0914

logger = logging.getLogger(__name__)


"""
source: github.com/rcsb/weekly-update-workflow
"""


class ContentTypeEnum(Enum):
    EXPERIMENTAL = "experimental"
    COMPUTATIONAL = "computational"


class WorkflowUtilities:

    def __init__(
        self,
        coast: str = None,
        interpolation: bool = True,
        out: str = None,
        prereleaseFtpFileBasePath: str = None,
        pdbIdsTimestampFilePath: str = None,
        csmFileRepoBasePath: str = None,
        csmHoldingsUrl: str = None,
        structureFilePath: str = None,
    ):
        self.contentTypeDir: Dict = {
            ContentTypeEnum.EXPERIMENTAL.value: "pdb",
            ContentTypeEnum.COMPUTATIONAL.value: "csm",
        }
        self.whatCoast = coast
        self.appendCoastToFtpFileBasePath = bool(interpolation)
        self.updateBase = out
        self.prereleaseFtpFileBasePath = prereleaseFtpFileBasePath
        self.pdbIdsTimestampFilePath = pdbIdsTimestampFilePath
        self.csmFileRepoBasePath = csmFileRepoBasePath
        self.csmHoldingsUrl = csmHoldingsUrl
        self.structureFilePath = structureFilePath

    # list file download

    def get_pdb_list(self, load_type: str) -> list:
        pdb_ids_timestamps, url = self.get_all_current_pdb_ids_with_timestamps()
        logger.info("read timestamps from %s", url)
        pdb_list = []
        content_type = ContentTypeEnum.EXPERIMENTAL.value
        # /mnt/models/update-store/pdb
        # changed to
        # /mnt/vdb1/out/pdb
        base_dir = os.path.join(self.updateBase, self.contentTypeDir[content_type])
        if load_type == "full":
            logger.info("running full workflow")
            for pdb_id in pdb_ids_timestamps:
                pdb_path = pdb_id[1:3] + "/" + pdb_id + ".cif.gz"
                pdb_list.append("%s %s %s" % (pdb_id, pdb_path, content_type))
        else:
            # 'incremental' for weekly
            logger.info("running incremental workflow")
            for pdb_id, cif_timestamp in pdb_ids_timestamps.items():
                zip_cif_path = pdb_id[1:3] + "/" + pdb_id + ".cif.gz"
                bcif_path = pdb_id[1:3] + "/" + pdb_id + ".bcif"
                zip_bcif_path = pdb_id[1:3] + "/" + pdb_id + ".bcif.gz"
                # /mnt/models/update-store/pdb/1o08/1o08.bcif.gz
                # changed to
                # /mnt/vdb1/out/pdb/1o08/1o08.bcif.gz
                bcif_file = os.path.join(base_dir, bcif_path)
                zip_bcif_file = os.path.join(base_dir, zip_bcif_path)
                # test pre-existence and modification time
                # allow option to output either .bcif or .bcif.gz files (determined by default at time of file write)
                # return .cif.gz file paths for download rather than bcif output file
                if os.path.exists(bcif_file):
                    t1 = os.path.getmtime(bcif_file)
                    t2 = cif_timestamp.timestamp()
                    if t1 < t2:
                        pdb_list.append(
                            "%s %s %s" % (pdb_id, zip_cif_path, content_type)
                        )
                elif os.path.exists(zip_bcif_file):
                    t1 = os.path.getmtime(zip_bcif_file)
                    t2 = cif_timestamp.timestamp()
                    if t1 < t2:
                        pdb_list.append(
                            "%s %s %s" % (pdb_id, zip_cif_path, content_type)
                        )
                else:
                    pdb_list.append("%s %s %s" % (pdb_id, zip_cif_path, content_type))
        return pdb_list

    def get_all_current_pdb_ids_with_timestamps(self) -> Tuple[dict, str]:
        times_dic = {}
        # http://prereleaseftp-%s.rcsb.org/pdb % east, holdings/released_structures_last_modified_dates.json.gz
        all_pdb_ids_url = os.path.join(
            self.get_prerelease_ftp_file_base_url(), self.pdbIdsTimestampFilePath
        )
        with urllib.request.urlopen(all_pdb_ids_url) as url:
            data = json.loads(gzip.decompress(url.read()))
            for pdb_id in data:
                try:
                    datetime_object = datetime.datetime.strptime(
                        data[pdb_id], "%Y-%m-%dT%H:%M:%S%z"
                    )
                except ValueError:
                    # requires python >= 3.12
                    # datetime_object = datetime.datetime.strptime(data[pdb_id], "%Y-%m-%dT%H:%M:%S%:z")
                    dat = data[pdb_id][0:-6]
                    offset = data[pdb_id][-6:]
                    if offset.find(":") >= 0:
                        offset = offset.replace(":", "")
                    dat = "%s%s" % (dat, offset)
                    datetime_object = datetime.datetime.strptime(
                        dat, "%Y-%m-%dT%H:%M:%S%z"
                    )
                times_dic[pdb_id.lower()] = datetime_object
        return times_dic, all_pdb_ids_url

    def get_prerelease_ftp_file_base_url(self) -> str:
        if self.appendCoastToFtpFileBasePath:  # default False
            # http://prereleaseftp-%s.rcsb.org/pdb % east
            return self.prereleaseFtpFileBasePath % self.get_what_coast_we_are()
        # http://prereleaseftp-%s.rcsb.org/pdb
        return self.prereleaseFtpFileBasePath

    def get_what_coast_we_are(self) -> str:
        coast_path = self.whatCoast
        if coast_path is None or coast_path == "":
            raise ValueError("The whatCoast parameter was not passed in luigi CLI.")
        if coast_path not in ("east", "west"):
            raise ValueError(
                "The whatCoast parameter was not passed correctly in luigi CLI. It must be either 'east' or 'west'"
            )
        return coast_path

    def get_comp_list(self, load_type) -> list:
        model_ids_metadata = self.get_all_current_model_ids_with_metadata()
        if not model_ids_metadata:
            return None
        model_list = []
        content_type = ContentTypeEnum.COMPUTATIONAL.value
        # /mnt/models/update-store/csm
        # changed to
        # /mnt/vdb1/out/csm
        base_dir = os.path.join(self.updateBase, self.contentTypeDir[content_type])
        if load_type == "full":
            for model_id, metadata in model_ids_metadata.items():
                model_path = metadata["modelPath"]
                model_list.append("%s %s %s" % (model_id, model_path, content_type))
        else:
            # 'incremental' for weekly
            for model_id, metadata in model_ids_metadata.items():
                model_path = metadata["modelPath"]
                bcif_model_path = (
                    metadata["modelPath"]
                    .replace(".cif.gz", ".bcif")
                    .replace(".cif", ".bcif")
                )
                bcif_zip_path = (
                    metadata["modelPath"]
                    .replace(".cif.gz", ".bcif.gz")
                    .replace(".cif", ".bcif.gz")
                )
                # /mnt/models/update-store/csm/1o08/1o08.bcif.gz
                # changed to
                # /mnt/vdb1/out/csm/1o08/1o08.bcif.gz
                bcif_file = os.path.join(base_dir, bcif_model_path)
                bcif_zip_file = os.path.join(base_dir, bcif_zip_path)
                # check pre-existence and modification time
                # enable output of either .bcif or .bcif.gz files (determined by default at time of file write)
                # return cif model path for download rather than output bcif filepath
                if os.path.exists(bcif_file):
                    t1 = os.path.getmtime(bcif_file)
                    t2 = metadata["datetime"].timestamp()
                    if t1 < t2:
                        model_list.append(
                            "%s %s %s" % (model_id, model_path, content_type)
                        )
                elif os.path.exists(bcif_zip_file):
                    t1 = os.path.getmtime(bcif_zip_file)
                    t2 = metadata["datetime"].timestamp()
                    if t1 < t2:
                        model_list.append(
                            "%s %s %s" % (model_id, model_path, content_type)
                        )
                else:
                    model_list.append("%s %s %s" % (model_id, model_path, content_type))
        return model_list

    def get_all_current_model_ids_with_metadata(self) -> dict:
        try:
            # http://computed-models-internal-%s.rcsb.org/staging % east, holdings/computed-models-holdings.json.gz
            holdingsFileUrl = os.path.join(
                self.get_csm_file_repo_base_url(), self.csmHoldingsUrl
            )
            dic = {}
            with urllib.request.urlopen(holdingsFileUrl) as url:
                data = json.loads(gzip.decompress(url.read()))
                for model_id in data:
                    item = data[model_id]
                    item["modelPath"] = item[
                        "modelPath"
                    ].lower()  # prod route of BinaryCIF wf produces lowercase filenames
                    item["datetime"] = datetime.datetime.strptime(
                        item["lastModifiedDate"], "%Y-%m-%dT%H:%M:%S%z"
                    )
                    dic[model_id.lower()] = item
            return dic
        except Exception as e:
            logger.exception(str(e))
            return None

    def get_csm_file_repo_base_url(self) -> str:
        if self.appendCoastToFtpFileBasePath:  # default False
            # http://computed-models-internal-%s.rcsb.org/staging % east
            return self.csmFileRepoBasePath % self.get_what_coast_we_are()
        # http://computed-models-internal-%s.rcsb.org/staging
        return self.csmFileRepoBasePath

    # cif file download

    def get_download_url(self, input_file, content_type) -> str:
        # http://prereleaseftp-%s.rcsb.org/pdb % east
        base_url = self.get_file_repo_base_url_content_type_aware(content_type)
        if content_type == ContentTypeEnum.EXPERIMENTAL.value:
            cifgz_url = os.path.join(base_url, self.structureFilePath)
        else:
            cifgz_url = base_url + "/"
        cifgz_url += input_file
        return cifgz_url

    def get_file_repo_base_url_content_type_aware(
        self, content_type=ContentTypeEnum.EXPERIMENTAL.value
    ) -> str:
        if content_type == ContentTypeEnum.EXPERIMENTAL.value:
            return self.get_prerelease_ftp_file_base_url()
        if content_type == ContentTypeEnum.COMPUTATIONAL.value:
            return self.get_csm_file_repo_base_url()
        raise ValueError(
            "Unsupported value for 'content_type' parameter: '%s'" % content_type
        )
