##
# File:    OpenFold3ModelCif.py
# Author:  Jose Duarte
# Date:    9-Oct-2026
##

"""
Convert OpenFold3 predictions to ModelCIF, written as BinaryCIF.

For every query of an OpenFold3 run, takes the best samples (by default the one with
the highest sample_ranking_score) and writes:

    <query>_model.bcif.gz   ModelCIF: the samples as ranked models, with coordinates,
                            per-residue pLDDT (mean over the residue's atoms), global
                            pLDDT and pTM
    <query>_pae.bcif.gz     optional associated file with the models' full PAE matrices,
                            linked from the model file (ma_entry_associated_files)

Files are gzipped by default (.bcif without). PAE is by far the largest part of the
output (one row per residue pair), so it is rounded to multiples of a step, 0.5 Angstrom
by default (OpenFold3's PAE bin width). That only pays off with gzip: BinaryCIF takes
about 2 bytes per PAE row at any step, while gzip compresses the coarser values much
better (1010 residues: 2.1 MB uncompressed at any step; gzipped 1.1 MB at 0.01 A and
0.34 MB at 0.5 A). pLDDT, per atom (B-factor column) and per residue, is rounded to
multiples of 0.1 by default, which makes the model file about 14% smaller.

ModelCIF is assembled with python-modelcif and converted to BinaryCIF with py-mmcif
(>= 1.2.0, for fixed-point encoding of float columns). Text mmCIF is only buffered
in memory; just the two BinaryCIF files are written to disk.
"""

__docformat__ = "google en"
__author__ = "Jose Duarte"
__email__ = "jose.duarte@rcsb.org"
__license__ = "Apache 2.0"

import decimal
import glob
import gzip
import io
import json
import logging
import math
import os
import re
import shutil
import tempfile

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


class ModelCifConversionError(Exception):
    """One or more queries of an OpenFold3 run could not be converted.

    Attributes:
        errors (dict): query name to the exception that stopped its conversion
    """

    def __init__(self, errors):
        self.errors = errors
        details = "; ".join("%s: %s: %s" % (query, type(err).__name__, err) for query, err in errors.items())
        super().__init__("ModelCIF conversion failed for %d queries: %s" % (len(errors), details))


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


def roundToStep(value, step):
    """Round value half up to the nearest multiple of step, without float noise (0.3, not 0.30000000000000004)."""
    decimals = max(0, -decimal.Decimal(str(step)).as_tuple().exponent)
    return round(math.floor(value / step + 0.5) * step, decimals)


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


def getRankedSamples(queryDir, rankBy="sample_ranking_score", descending=True):
    """Return the samples under an OpenFold3 query directory, best first.

    Args:
        queryDir (str): OpenFold3 query directory, holding seed_* directories
        rankBy (str): key of the aggregated confidence scores to rank by
        descending (bool): True if higher values are better

    Returns:
        list: (model mmCIF path, sample path prefix, aggregated scores) per sample, best first; ties keep file order
    """
    samples = []
    for aggFile in sorted(glob.glob(os.path.join(queryDir, "seed_*", "*_confidences_aggregated.json"))):
        with open(aggFile, "r", encoding="utf-8") as fh:
            scores = json.load(fh)
        if rankBy not in scores:
            msg = "%s has no score %r; available: %s" % (aggFile, rankBy, sorted(scores))
            raise KeyError(msg)
        prefix = aggFile[: -len("_confidences_aggregated.json")]
        samples.append((prefix + "_model.cif", prefix, scores))
    return sorted(samples, key=lambda s: s[2][rankBy], reverse=descending)


def _sampleName(prefix):
    """OpenFold3 seed and sample of a sample path prefix, e.g. 'seed 42 sample 1'."""
    match = re.search(r"_seed_(\d+)_sample_(\d+)$", os.path.basename(prefix))
    return "seed %s sample %s" % match.groups() if match else os.path.basename(prefix)


def _readResidues(modelCifPath):
    """Atom rows of an OpenFold3 mmCIF model grouped by residue, in sequence order."""
    residues = {}
    for row in readAtomSite(modelCifPath):
        residues.setdefault(int(row["label_seq_id"]), []).append(row)
    return [residues[i] for i in sorted(residues)]


def buildModelCif(query, samples, of3Version, checkpoint, rankBy="sample_ranking_score", paeStep=0.5, gzipped=True, plddtStep=0.1):
    """Return the ModelCIF system of one OpenFold3 query, with one model per sample.

    Args:
        query (str): query name, e.g. 1a6y_A
        samples (list): (model mmCIF path, sample path prefix, aggregated scores, PAE matrix or None) per
            sample, in rank order. Models have per-atom pLDDT in the B-factor column; PAE has one row per
            token (residue). All samples must be of the same sequence.
        of3Version (str): OpenFold3 version
        checkpoint (str): OpenFold3 checkpoint file name
        rankBy (str): score the samples were ranked by, recorded in the model names
        paeStep (float): PAE values are rounded to multiples of this step, in Angstrom
        gzipped (bool): the PAE file will be gzipped, so it is linked as <query>_pae.bcif.gz
        plddtStep (float): per-atom (B-factor) and per-residue pLDDT values are rounded to multiples of this step;
            per-residue values are means of the unrounded per-atom values

    Returns:
        modelcif.System: models in rank order in one model group; if there is PAE, the associated file
        is only linked, as <query>_pae.bcif(.gz), and writeBcif writes it
    """
    if paeStep <= 0:
        msg = "PAE step must be positive, got %r" % paeStep
        raise ValueError(msg)
    if plddtStep <= 0:
        msg = "pLDDT step must be positive, got %r" % plddtStep
        raise ValueError(msg)
    first = _readResidues(samples[0][0])
    compIds = [res[0]["label_comp_id"] for res in first]
    alphabet = ihm.LPeptideAlphabet()
    entity = modelcif.Entity([_chemComp(alphabet, c) for c in compIds], description="%s model" % query)
    asym = modelcif.AsymUnit(entity, details="Model of %s" % query, id="A")
    assembly = modelcif.Assembly([asym], name="Modeled assembly")

    models = []
    hasPae = False
    for rank, (modelCifPath, prefix, scores, pae) in enumerate(samples, start=1):
        residues = first if rank == 1 else _readResidues(modelCifPath)
        if [res[0]["label_comp_id"] for res in residues] != compIds:
            msg = "%s: %s is not of the same sequence as the top-ranked sample" % (query, modelCifPath)
            raise ValueError(msg)
        atoms = [
            modelcif.model.Atom(
                asym_unit=asym,
                seq_id=seqId,
                atom_id=r["label_atom_id"],
                type_symbol=r["type_symbol"],
                x=float(r["Cartn_x"]),
                y=float(r["Cartn_y"]),
                z=float(r["Cartn_z"]),
                biso=roundToStep(float(r["B_iso_or_equiv"]), plddtStep),
                occupancy=1.0,
                het=r["group_PDB"] == "HETATM",
            )
            for seqId, res in enumerate(residues, start=1)
            for r in res
        ]
        name = "OpenFold3 %s, rank %d by %s" % (_sampleName(prefix), rank, rankBy)
        model = _OpenFold3Model(atoms, assembly=assembly, name=name)

        # per-atom pLDDT is in the B-factor column; average it per residue
        model.qa_metrics.append(_GlobalPLDDT(scores["avg_plddt"]))
        model.qa_metrics.append(_GlobalPTM(scores["ptm"]))
        for seqId, res in enumerate(residues, start=1):
            plddt = sum(float(a["B_iso_or_equiv"]) for a in res) / len(res)
            model.qa_metrics.append(_LocalPLDDT(asym.residue(seqId), roundToStep(plddt, plddtStep)))

        if pae is not None:
            if len(pae) != len(residues):
                # PAE is per token, one per residue only for standard polymer residues
                msg = "%s: PAE has %d tokens but the model %d residues" % (query, len(pae), len(residues))
                raise ValueError(msg)
            for i, paeRow in enumerate(pae, start=1):
                for j, value in enumerate(paeRow, start=1):
                    model.qa_metrics.append(_LocalPairwisePAE(asym.residue(i), asym.residue(j), roundToStep(value, paeStep)))
            hasPae = True
        models.append(model)

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
    _LocalPLDDT.description = "Per-residue pLDDT, mean over the residue's atoms; per-atom pLDDT is in the B-factor column. Both rounded to multiples of %g" % plddtStep
    _LocalPairwisePAE.description = "Predicted aligned error, in Angstrom, rounded to multiples of %g" % paeStep

    system = modelcif.System(title="OpenFold3 prediction of %s" % query, id=query.upper())
    system.authors.append("RCSB PDB")
    group = modelcif.model.ModelGroup(models, name="OpenFold3 prediction, ranked by %s" % rankBy)
    system.model_groups.append(group)
    protocol = modelcif.protocol.Protocol()
    protocol.steps.append(
        modelcif.protocol.ModelingStep(
            input_data=modelcif.data.DataGroup([entity]),
            output_data=group,
            software=software,
            name="OpenFold3 inference",
            details="Diffusion samples of an OpenFold3 prediction, the best %d by %s." % (len(models), rankBy),
        )
    )
    system.protocols.append(protocol)

    if hasPae:
        # Only the link: with no categories given, modelcif does not write the file
        # itself (it would write it to disk); writeBcif moves the PAE category there.
        system.repositories.append(
            modelcif.associated.Repository(
                "",
                [modelcif.associated.QAMetricsFile(_bcifName(query, "pae", gzipped), binary=True, details="Predicted aligned error")],
            )
        )
    return system


def _bcifName(query, kind, gzipped):
    """Output file name, e.g. 1a6y_A_pae.bcif.gz."""
    return "%s_%s.bcif%s" % (query, kind, ".gz" if gzipped else "")


def _serialize(writer, filePath, containers):
    """Write containers as BinaryCIF, gzipped if filePath ends in .gz; the file appears only once complete."""
    fd, tmpPath = tempfile.mkstemp(suffix=".bcif", dir=os.path.dirname(os.path.abspath(filePath)))
    os.close(fd)
    try:
        # serialize logs errors and returns False rather than raising
        if not writer.serialize(tmpPath, containers):
            msg = "Failed to write BinaryCIF %s" % filePath
            raise IOError(msg)
        if filePath.endswith(".gz"):
            with open(tmpPath, "rb") as src, gzip.open(filePath, "wb") as dst:
                shutil.copyfileobj(src, dst)
        else:
            os.replace(tmpPath, filePath)
    finally:
        if os.path.exists(tmpPath):
            os.remove(tmpPath)


def writeBcif(system, modelPath, paePath=None):
    """Write a ModelCIF system as BinaryCIF: the model file and, if the system has PAE, the associated PAE file.

    Paths ending in .gz are gzipped. The whole system is dumped to an in-memory mmCIF
    buffer and parsed with py-mmcif; the pairwise QA category then moves to its own
    container, with an entry and entry_link header like modelcif gives associated files.
    """
    buffer = io.StringIO()
    modelcif.dumper.write(buffer, [system])
    buffer.seek(0)
    containers = []
    PdbxReader(buffer).read(containers)
    model = containers[0]
    writer = BinaryCifWriter(dictionaryApi=None)

    pairwise = model.getObj(PAIRWISE_CATEGORY)
    if pairwise is not None:
        if paePath is None:
            msg = "%s has PAE but no PAE file path was given" % system.id
            raise ValueError(msg)
        model.remove(PAIRWISE_CATEGORY)
        paeContainer = DataContainer("%s_pae" % system.id)
        paeContainer.append(DataCategory("entry", ["id"], [["%s_pae" % system.id]]))
        paeContainer.append(DataCategory("entry_link", ["id", "entry_id", "details"], [["1", system.id, "Predicted aligned error of the models"]]))
        paeContainer.append(pairwise)
        _serialize(writer, paePath, [paeContainer])
    _serialize(writer, modelPath, [model])


def findQueryDirs(of3OutputDir):
    """Return the OpenFold3 query directories (those holding seed_* directories) under of3OutputDir, at any depth."""
    seedDirs = glob.glob(os.path.join(of3OutputDir, "**", "seed_*"), recursive=True)
    return sorted({os.path.dirname(d) for d in seedDirs if os.path.isdir(d)})


def convertOpenFold3Outputs(
    of3OutputDir, outputDir, of3Version, checkpoint, numModels=1, rankBy="sample_ranking_score", descending=True, withPae=True, paeStep=0.5, gzipped=True, plddtStep=0.1
):
    """Convert the best samples of every query of an OpenFold3 run to ModelCIF BinaryCIF.

    Writes <query>_model.bcif.gz per query, with the selected samples as ranked models, and with
    withPae <query>_pae.bcif.gz, the associated file with their PAE matrices (.bcif without gzipped).

    Args:
        of3OutputDir (str): OpenFold3 output directory; query directories (<query>/seed_*) are found at any depth
        outputDir (str): directory for the output files
        of3Version (str): OpenFold3 version, recorded in the ModelCIF software
        checkpoint (str): OpenFold3 checkpoint file name, recorded in the ModelCIF software
        numModels (int): number of samples per query, best first; 0 for all
        rankBy (str): key of the OpenFold3 aggregated confidence scores to rank samples by
        descending (bool): True if higher values of rankBy are better
        withPae (bool): include PAE, read from the samples' full confidences (*_confidences.json)
        paeStep (float): PAE values are rounded to multiples of this step, in Angstrom
        gzipped (bool): gzip the output files; needed for coarser paeStep values to make the PAE file smaller
        plddtStep (float): per-atom (B-factor) and per-residue pLDDT values are rounded to multiples of this step

    Returns:
        list: the converted queries

    Raises:
        ModelCifConversionError: if any query could not be converted, after trying all others
    """
    os.makedirs(outputDir, exist_ok=True)
    converted = []
    errors = {}
    for queryDir in findQueryDirs(of3OutputDir):
        query = os.path.basename(queryDir)
        try:
            ranked = getRankedSamples(queryDir, rankBy, descending)
            if not ranked:
                msg = "no OpenFold3 sample outputs in %s" % queryDir
                raise ValueError(msg)
            selected = ranked[:numModels] if numModels > 0 else ranked
            samples = []
            for modelCifPath, prefix, scores in selected:
                pae = None
                if withPae:
                    with open(prefix + "_confidences.json", "r", encoding="utf-8") as fh:
                        pae = json.load(fh)["pae"]
                samples.append((modelCifPath, prefix, scores, pae))
            system = buildModelCif(query, samples, of3Version, checkpoint, rankBy, paeStep=paeStep, gzipped=gzipped, plddtStep=plddtStep)
            writeBcif(
                system,
                os.path.join(outputDir, _bcifName(query, "model", gzipped)),
                os.path.join(outputDir, _bcifName(query, "pae", gzipped)) if withPae else None,
            )
            logger.info("%s: converted %s", query, ", ".join(_sampleName(prefix) for _, prefix, _, _ in samples))
            converted.append(query)
        except Exception as err:  # pylint: disable=broad-exception-caught
            # one query's failure must not stop the others; all failures are raised together below
            errors[query] = err
    if errors:
        # chained to the first failure, so its traceback is kept
        raise ModelCifConversionError(errors) from next(iter(errors.values()))
    return converted
