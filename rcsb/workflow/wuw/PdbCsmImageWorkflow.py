"""Images workflow functions that actually do the work."""

import gzip
import json
import datetime
import random
from pathlib import Path
import logging
import subprocess

logger = logging.getLogger(__name__)


def get_pdb_list(pdb_gz_path: str, update_all_images: bool, pdb_base_dir: str) -> list:
    """Build pdb list via pdb gz file."""
    pdb_ids_timestamps = {}
    with gzip.open(pdb_gz_path) as f:
        data = json.loads(f.read())
        for id_val in data:
            datetime_object = datetime.datetime.strptime(data[id_val], "%Y-%m-%dT%H:%M:%S%z")
            pdb_ids_timestamps[id_val.lower()] = datetime_object
    pdb_id_list = []
    if update_all_images:
        for id_val in pdb_ids_timestamps:
            path = id_val[1:3] + "/" + id_val + ".bcif"
            pdb_id_list.append(f"{id_val} {path} experimental")
    else:
        for id_val, timestamp in pdb_ids_timestamps.items():
            path = id_val[1:3] + "/" + id_val + ".bcif"
            bcif_file = pdb_base_dir + path
            if Path.exists(bcif_file):
                t1 = Path.stat(bcif_file).st_mtime
                t2 = timestamp.timestamp()
                if t1 < t2:
                    pdb_id_list.append(f"{id_val} {path} experimental")
            else:
                pdb_id_list.append(f"{id_val} {path} experimental")
    return pdb_id_list


def get_csm_list(csm_gz_path: str, update_all_images: bool, csm_base_dir: str) -> list:
    """Build csm list via csm gz file."""
    with gzip.open(csm_gz_path) as f:
        data = json.loads(f.read())
        dic = {}
        for model_id in data:
            item = data[model_id]
            item["modelPath"] = item["modelPath"].lower()  # prod route of BinaryCIF wf produces lowercase filenames
            item["datetime"] = datetime.datetime.strptime(item["lastModifiedDate"], "%Y-%m-%dT%H:%M:%S%z")
            dic[model_id.lower()] = item
    model_ids_metadata = dic
    model_list = []

    if update_all_images:
        for model_id, metadata in model_ids_metadata.items():
            model_path = metadata["modelPath"].replace(".cif", ".bcif").replace('.gz', '')
            model_list.append(f"{model_id} {model_path} computational")
    else:
        # 'incremental' for weekly
        for model_id, metadata in model_ids_metadata.items():
            model_path = metadata["modelPath"].replace(".cif", ".bcif").replace('.gz', '')
            bcif_file = csm_base_dir + model_path
            if Path.exists(bcif_file):
                t1 = Path.stat(bcif_file).st_mtime
                t2 = metadata["datetime"].timestamp()
                if t1 < t2:
                    model_list.append(f"{model_id} {model_path} computational")
            else:
                model_list.append(f"{model_id} {model_path} computational")
    return model_list


def images_gen_lists(args: dict) -> None:
    """Generate lists of pdbs/csms in files."""
    pdb_id_list = get_pdb_list(pdb_gz_path=args['pdb_gz_path'], update_all_images=args['update_all_images'], pdb_base_dir=args['pdb_base_dir'])
    comp_id_list = [] if args["imgs_exclude_models"] else get_csm_list(csm_gz_path=args['csm_gz_path'], update_all_images=['update_all_images'], csm_base_dir=args['csm_base_dir'])

    # Print results, combine, and shuffle
    if len(pdb_id_list) < 1 and len(comp_id_list) < 1:
        msg = "pdb and csm id list empty"
        raise ValueError(msg)
    msg = f"There are {len(pdb_id_list)} pdb_ids and {len(comp_id_list)} comp_ids for which to generate BCIF and images"

    full_id_list = pdb_id_list + comp_id_list
    random.shuffle(full_id_list)

    steps = int(len(full_id_list) / int(args["num_workers"]) )
    for i in range(0, len(full_id_list), steps):
        Path(args["id_list_path"]).mkdir(parents=True, exist_ok=True)
        with Path.open(args["id_list_path"] + str(int(i/steps)), "w") as f:
            for line in full_id_list[i : i + steps]:
                f.write(line + "\n")

def images_gen_jpgs(args: dict) -> None:
    """Generate jpgs for given pdb/csm list."""
    with Path.open(args["id_list_path"] + args['file_number'], 'r') as f:
        id_list = [line.rstrip('\n') for line in f]
    if not isinstance(id_list, list):
        msg = "id_list not a list"
        raise TypeError(msg)
    
    for line in id_list:
        # Requirements:
        # 1. bcif files must be unzipped
        # 2. bcif files must be in a local dir (otherwise I'll have to add a curl step)
        # 3. bcif files do not need conversion from cif files. This should be taken care of in bcif workflow
        # 
        # todo: add a step that checks:
        # 1. the dir exists
        # 2. the file is a bcif with data inside of it
        # 3. the output dir is availible

        file_id, bcif_file_name, sdm = line.split(" ")
        content_type_dir = "pdb/" if sdm == "experimental" else "csm/"

        bcif_file_path = args["pdb_base_dir"] + bcif_file_name if sdm == "experimental" else args["csm_base_dir"] + bcif_file_name 
        
        out_path = args["jpgs_out_dir"] + content_type_dir
        Path(out_path).mkdir(parents=True, exist_ok=True)
        
        ### run_molrender ###
        cmd = [
            args['jpg_xvfb_executable'],
            '-a',
            '-s', f'-ac -screen 0 {args['jpg_screen']}',
            args["molrender_exe"],
            'all',
            bcif_file_path,
            out_path,
            '--height', args['jpg_height'],
            '--width', args['jpg_width'],
            '--format', args['jpg_format'],
            ]
        if args['jpg_additional_cmds'] is None:
            cmd = [*cmd, args['jpg_additional_cmds']]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            logger.info("Command was successful!")
            logger.info(result.stdout)
        except subprocess.CalledProcessError as e:
            msg = f"Command failed with exit code {e.returncode} \n Error output: {e.stderr}"
            logging.exception(msg)
            raise

        ### check result ###
        out_jpg_file = out_path + file_id + "_model-1.jpeg" 

        if Path(out_jpg_file).is_file() and Path(out_jpg_file).stat().st_size > 0:
            logger.info("Got the image file %s.", out_jpg_file)
        else:
            logger.warning("No image file: %s.", out_jpg_file)

        # Potentially will need to zip afterwards. 
        # This could be its own task
        #### ZIP
        # cmd = ["gzip", "-f", out_file]
        # run(cmd)
