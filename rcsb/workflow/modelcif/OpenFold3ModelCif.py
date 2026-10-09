##
# File:    OpenFold3ModelCif.py
# Author:  Jose Duarte
# Date:    9-Oct-2026
##

"""
Convert OpenFold3 predictions to ModelCIF, written as BinaryCIF.

For every query of an OpenFold3 run, takes the top-ranked sample (highest
sample_ranking_score) and writes two files:

    <query>_model.bcif   ModelCIF: coordinates, per-residue pLDDT (mean over the
                         residue's atoms), global pLDDT and pTM
    <query>_pae.bcif     associated file with the full PAE matrix, linked from the
                         model file (ma_entry_associated_files)

ModelCIF is assembled with python-modelcif and converted to BinaryCIF with py-mmcif
(>= 1.2.0, for fixed-point encoding of float columns). Text mmCIF is only buffered
in memory; just the two BinaryCIF files are written to disk.
"""

__docformat__ = "google en"
__author__ = "Jose Duarte"
__email__ = "jose.duarte@rcsb.org"
__license__ = "Apache 2.0"

import glob
import io
import json
import logging
import os

import ihm
import modelcif
import modelcif.associated
import modelcif.data
import modelcif.dumper
import modelcif.model
import modelcif.protocol
import modelcif.qa_metric
from mmcif.api.DataCategory import DataCategory
from mmcif.api.PdbxContainers import DataContainer
from mmcif.io.BinaryCifWriter import BinaryCifWriter
from mmcif.io.IoAdapterPy import IoAdapterPy
from mmcif.io.PdbxReader import PdbxReader

logger = logging.getLogger(__name__)

OPENFOLD3_URL = "https://github.com/aqlaboratory/openfold-3"
# the category moved from the model file to the associated PAE file
PAIRWISE_CATEGORY = "ma_qa_metric_local_pairwise"


class _OpenFold3Model(modelcif.model.AbInitioModel):
    """Model whose atoms come from the OpenFold3 mmCIF atom_site table."""

    def __init__(self, atoms, **kwargs):
        super().__init__(**kwargs)
        self._atoms = atoms

    def get_atoms(self):
        return iter(self._atoms)


class _GlobalPLDDT(modelcif.qa_metric.Global, modelcif.qa_metric.PLDDT):
    name = "pLDDT"
    description = "Mean pLDDT over all atoms of the model"


class _LocalPLDDT(modelcif.qa_metric.Local, modelcif.qa_metric.PLDDT):
    name = "pLDDT"
    description = "Per-residue pLDDT, mean over the residue's atoms"


class _GlobalPTM(modelcif.qa_metric.Global, modelcif.qa_metric.PTM):
    name = "pTM"
    description = "Predicted TM-score"


class _LocalPairwisePAE(modelcif.qa_metric.LocalPairwise, modelcif.qa_metric.PAE):
    name = "PAE"
    description = "Predicted aligned error, in Angstrom"


def readAtomSite(filePath):
    """Return the atom_site rows of an mmCIF file as dicts."""
    [container] = IoAdapterPy().readFile(filePath)
    cat = container.getObj("atom_site")
    names = cat.getAttributeList()
    return [dict(zip(names, row)) for row in cat.getRowList()]


def _chemComp(alphabet, compId):
    """Chemical component for a residue name, standard or not."""
    for comp in alphabet._comps.values():  # pylint: disable=protected-access
        if comp.id == compId:
            return comp
    return ihm.LPeptideChemComp(compId, "X", "X")


def getTopSample(queryDir):
    """Return (model mmCIF path, sample path prefix, aggregated scores) of the
    sample with the highest sample_ranking_score under an OpenFold3 query directory,
    or None if there is none."""
    best, bestScore = None, None
    for aggFile in sorted(glob.glob(os.path.join(queryDir, "seed_*", "*_confidences_aggregated.json"))):
        with open(aggFile, "r", encoding="utf-8") as fh:
            scores = json.load(fh)
        prefix = aggFile[: -len("_confidences_aggregated.json")]
        if bestScore is None or scores["sample_ranking_score"] > bestScore:
            best, bestScore = (prefix + "_model.cif", prefix, scores), scores["sample_ranking_score"]
    return best


def buildModelCif(query, modelCifPath, pae, scores, of3Version, checkpoint):
    """Return the ModelCIF system of one OpenFold3 prediction.

    Args:
        query (str): query name, e.g. 1a6y_A
        modelCifPath (str): OpenFold3 mmCIF model, with per-atom pLDDT in the B-factor column
        pae (list): PAE matrix, one row per token (residue)
        scores (dict): OpenFold3 aggregated confidence scores of the sample
        of3Version (str): OpenFold3 version
        checkpoint (str): OpenFold3 checkpoint file name

    Returns:
        modelcif.System: the PAE file is only linked, as <query>_pae.bcif; writeBcif writes it
    """
    rows = readAtomSite(modelCifPath)
    residues = {}
    for row in rows:
        residues.setdefault(int(row["label_seq_id"]), []).append(row)
    seqIds = sorted(residues)
    compIds = [residues[i][0]["label_comp_id"] for i in seqIds]

    alphabet = ihm.LPeptideAlphabet()
    entity = modelcif.Entity([_chemComp(alphabet, c) for c in compIds], description="%s model" % query)
    asym = modelcif.AsymUnit(entity, details="Model of %s" % query, id="A")
    assembly = modelcif.Assembly([asym], name="Modeled assembly")

    atoms = [
        modelcif.model.Atom(
            asym_unit=asym,
            seq_id=seqIds.index(int(r["label_seq_id"])) + 1,
            atom_id=r["label_atom_id"],
            type_symbol=r["type_symbol"],
            x=float(r["Cartn_x"]),
            y=float(r["Cartn_y"]),
            z=float(r["Cartn_z"]),
            biso=float(r["B_iso_or_equiv"]),
            occupancy=1.0,
            het=r["group_PDB"] == "HETATM",
        )
        for r in rows
    ]
    model = _OpenFold3Model(atoms, assembly=assembly, name="Top-ranked OpenFold3 sample")

    # per-atom pLDDT is in the B-factor column; average it per residue
    plddt = [sum(float(a["B_iso_or_equiv"]) for a in residues[i]) / len(residues[i]) for i in seqIds]
    model.qa_metrics.append(_GlobalPLDDT(scores["avg_plddt"]))
    model.qa_metrics.append(_GlobalPTM(scores["ptm"]))
    for n, value in enumerate(plddt, start=1):
        model.qa_metrics.append(_LocalPLDDT(asym.residue(n), round(value, 2)))

    if len(pae) != len(seqIds):
        # PAE is per token, one per residue only for standard polymer residues
        msg = "%s: PAE has %d tokens but the model %d residues" % (query, len(pae), len(seqIds))
        raise ValueError(msg)
    for i, paeRow in enumerate(pae, start=1):
        for j, value in enumerate(paeRow, start=1):
            model.qa_metrics.append(_LocalPairwisePAE(asym.residue(i), asym.residue(j), round(value, 2)))

    software = modelcif.Software(
        name="OpenFold3",
        classification="model building",
        description="Structure prediction, checkpoint %s" % checkpoint,
        location=OPENFOLD3_URL,
        type="package",
        version=of3Version,
    )
    for metric in (_GlobalPLDDT, _LocalPLDDT, _GlobalPTM, _LocalPairwisePAE):
        metric.software = software

    system = modelcif.System(title="OpenFold3 prediction of %s" % query, id=query.upper())
    system.authors.append("RCSB PDB")
    group = modelcif.model.ModelGroup([model], name="OpenFold3 prediction")
    system.model_groups.append(group)
    protocol = modelcif.protocol.Protocol()
    protocol.steps.append(
        modelcif.protocol.ModelingStep(
            input_data=modelcif.data.DataGroup([entity]),
            output_data=group,
            software=software,
            name="OpenFold3 inference",
            details="Single-chain prediction from precomputed OpenProteinSet MSAs, no templates; the top-ranked of the diffusion samples.",
        )
    )
    system.protocols.append(protocol)

    # Only the link: with no categories given, modelcif does not write the file
    # itself (it would write it to disk); writeBcif moves the PAE category there.
    system.repositories.append(
        modelcif.associated.Repository(
            "",
            [modelcif.associated.QAMetricsFile("%s_pae.bcif" % query, binary=True, details="Predicted aligned error")],
        )
    )
    return system


def writeBcif(system, modelPath, paePath):
    """Write a ModelCIF system as two BinaryCIF files, the model and the associated PAE.

    The whole system is dumped to an in-memory mmCIF buffer and parsed with py-mmcif;
    the pairwise QA category then moves to its own container, with an entry and
    entry_link header like modelcif gives associated files.
    """
    buffer = io.StringIO()
    modelcif.dumper.write(buffer, [system])
    buffer.seek(0)
    containers = []
    PdbxReader(buffer).read(containers)
    model = containers[0]
    pairwise = model.getObj(PAIRWISE_CATEGORY)
    model.remove(PAIRWISE_CATEGORY)

    paeContainer = DataContainer("%s_pae" % system.id)
    paeContainer.append(DataCategory("entry", ["id"], [["%s_pae" % system.id]]))
    paeContainer.append(DataCategory("entry_link", ["id", "entry_id", "details"], [["1", system.id, "Predicted aligned error of the model"]]))
    paeContainer.append(pairwise)

    writer = BinaryCifWriter(dictionaryApi=None)
    writer.serialize(modelPath, [model])
    writer.serialize(paePath, [paeContainer])


def convertOpenFold3Outputs(of3OutputDir, queryJsonPaths, outputDir, of3Version, checkpoint):
    """Convert the top-ranked sample of every query of an OpenFold3 run to ModelCIF BinaryCIF.

    Args:
        of3OutputDir (str): OpenFold3 output directory, holding one or more job_*/<query> directories
        queryJsonPaths (list): OpenFold3 query json files, listing the queries to convert
        outputDir (str): directory for <query>_model.bcif and <query>_pae.bcif
        of3Version (str): OpenFold3 version, recorded in the ModelCIF software
        checkpoint (str): OpenFold3 checkpoint file name, recorded in the ModelCIF software

    Returns:
        list: queries that could not be converted; a failed query does not stop the others
    """
    queries = {}
    for queryJsonPath in queryJsonPaths:
        with open(queryJsonPath, "r", encoding="utf-8") as fh:
            queries.update(json.load(fh)["queries"])
    os.makedirs(outputDir, exist_ok=True)

    failed = []
    for query in sorted(queries):
        queryDirs = glob.glob(os.path.join(of3OutputDir, "job_*", query))
        top = getTopSample(queryDirs[0]) if queryDirs else None
        if top is None:
            logger.error("%s: no OpenFold3 outputs", query)
            failed.append(query)
            continue
        modelCifPath, prefix, scores = top
        try:
            with open(prefix + "_confidences.json", "r", encoding="utf-8") as fh:
                pae = json.load(fh)["pae"]
            system = buildModelCif(query, modelCifPath, pae, scores, of3Version, checkpoint)
            writeBcif(
                system,
                os.path.join(outputDir, "%s_model.bcif" % query),
                os.path.join(outputDir, "%s_pae.bcif" % query),
            )
            logger.info("%s: converted %s", query, os.path.basename(prefix))
        except Exception:  # pylint: disable=broad-exception-caught
            logger.exception("%s: ModelCIF conversion failed", query)
            failed.append(query)
    return failed
