"""
Command-line entry point for exporting or processing data from MongoDB.
"""

import logging
import os
import sys
import shutil
import time
from argparse import ArgumentParser, Namespace
from collections.abc import Generator, Sequence, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import ClassVar, Optional, Any

from bson.json_util import dumps as bson_dumps, RELAXED_JSON_OPTIONS, JSONOptions
from pymongo import MongoClient

logger = logging.getLogger("rcsb-workflow")


@dataclass(frozen=True, slots=True)
class UpdateInfo:
    path: Path
    timestamp: datetime


@dataclass(frozen=True, slots=True)
class IncrementalSource:
    """How to connect to MongoDB and what data to read for export."""

    client_factory: ClassVar[Callable[[str], MongoClient]] = MongoClient
    db_uri: str
    db_name: str
    collection: str
    fields: set[str]
    timestamp_field: Optional[str]

    def __call__(self, *, last_timestamp: Optional[datetime]) -> Generator[dict]:
        projection: Optional[dict[str, bool]] = None
        if self.fields:
            # PyMongo includes `_id` if `projection` is a list or doesn't contain a key `_id`.
            # To exclude `_id`, we need to specify `{"_id": False}`.
            projection = {field: True for field in self.fields}
            if "_id" not in self.fields:
                projection |= {"_id": False}
        query = {}
        if self.timestamp_field and last_timestamp:
            query[self.timestamp_field] = {"$gte": last_timestamp.isoformat()}
        with self.client_factory(self.db_uri) as client:
            db = client[self.db_name][self.collection]
            yield from db.find(query, projection=projection)


@dataclass(frozen=True, slots=True)
class IncrementalExporter:
    """Tool that reads a MongoDB ``source`` and writes to a file."""

    source: IncrementalSource
    json_options: JSONOptions = RELAXED_JSON_OPTIONS

    def export_to(self, since: Optional[datetime], previous_export: Optional[Path], out_file: Path) -> None:
        temp_file = self._get_temp_file(out_file)
        temp_file_2 = self._get_temp_file_2(out_file)
        logger.info(f"Reading documents from {self.source.collection}...")
        try:
            n_docs = self._export(since, temp_file)
            if previous_export:
                self._filter(previous_export, temp_file, temp_file_2)
                temp_file = temp_file_2
            shutil.move(temp_file, out_file)
        finally:
            temp_file_2.unlink(missing_ok=True)
            temp_file.unlink(missing_ok=True)
        logger.info(f"Wrote {n_docs} documents to {out_file}.")

    def _export(self, since: Optional[datetime], temp_file: Path) -> int:
        t0 = time.monotonic()
        logger.debug(f"Writing to temp file {temp_file}.")
        n_docs = 0
        with temp_file.open("w", encoding="utf-8", newline="\n") as f:
            f.write("[\n")
            for doc in self.source(last_timestamp=since):
                f.write(bson_dumps(doc, json_options=self.json_options))
                if n_docs > 0:
                    f.write(",\n")
                n_docs += 1
                if n_docs % 1000 == 0:
                    logger.debug(f"Wrote {n_docs} docs (Δt = {time.monotonic() - t0:.1} s).")
            f.write("\n]\n")
        logger.debug(f"Finished export. Wrote {n_docs} docs in {time.monotonic() - t0:.1} s.")
        return n_docs

    def _filter(self, previous_export: Path, temp_file: Path, out_file: Path) -> None:
        raise NotImplementedError()

    def _get_temp_file(self, out_file: Path) -> Path:
        return out_file.parent / f".{out_file.name}.raw.temp"

    def _get_temp_file_2(self, out_file: Path) -> Path:
        return out_file.parent / f".{out_file.name}.filtered.temp"
