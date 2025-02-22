"""
Command-line entry point for exporting or processing data from MongoDB.
"""

import logging
import os
import sys
import shutil
from argparse import ArgumentParser, Namespace
from collections.abc import Generator, Sequence, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import ClassVar, Optional

from bson.json_util import dumps as bson_dumps, RELAXED_JSON_OPTIONS, JSONOptions
from pymongo import MongoClient

logger = logging.getLogger("rcsb-workflow")


@dataclass(frozen=True, slots=True)
class Source:
    """How to connect to MongoDB and what data to read for export."""

    client_factory: ClassVar[Callable[[str], MongoClient]] = MongoClient
    db_uri: str
    db_name: str
    collection: str
    fields: set[str]
    skip: int = 0
    limit: int = 0

    def __call__(self) -> Generator[dict]:
        projection: Optional[dict[str, bool]] = None
        if self.fields:
            # PyMongo includes `_id` if `projection` is a list or doesn't contain a key `_id`.
            # To exclude `_id`, we need to specify `{"_id": False}`.
            projection = {field: True for field in self.fields}
            if "_id" not in self.fields:
                projection |= {"_id": False}
        with self.client_factory(self.db_uri) as client:
            db = client[self.db_name][self.collection]
            yield from db.find({}, projection=projection, skip=self.skip, limit=self.limit)


@dataclass(frozen=True, slots=True)
class Exporter:
    """App that reads a MongoDB ``source`` and writes to a file."""

    source: Source
    json_options: JSONOptions = RELAXED_JSON_OPTIONS

    def export_to(self, out_file: Path) -> None:
        temp_file = self._get_temp_file(out_file)
        try:
            n_docs = self._export(temp_file)
            shutil.move(temp_file, out_file)
        except BaseException:
            temp_file.unlink(missing_ok=True)
            raise
        logger.info(f"Wrote {n_docs} documents to {out_file}.")

    def _export(self, temp_file: Path) -> int:
        logger.info(f"Extracting documents from {self.source.collection}...")
        n_docs = 0
        with temp_file.open("w", encoding="utf-8", newline="\n") as f:
            f.write("[\n")
            for doc in self.source():
                f.write(bson_dumps(doc, json_options=self.json_options))
                if n_docs > 0:
                    f.write(",\n")
                n_docs += 1
                if n_docs % 1000 == 0:
                    logger.debug(f"Extracted {n_docs} documents.")
            f.write("\n]\n")
        return n_docs

    def _get_temp_file(self, out_file: Path) -> Path:
        return out_file.parent / f".{out_file.name}.temp"


@dataclass(frozen=True, slots=True)
class LogConfig:
    """Configuration for logging via CLI invocation."""

    format: str = "{asctime} {levelname:<7} {module}:{lineno} {message}"
    date_format: str = "%Y-%m-%d %H:%M:%S.%f"
    default_root_level: int = logging.WARNING
    default_level: int = logging.INFO

    def apply(self, *, verbose: int, quiet: int) -> None:
        """Applies a global logging configuration, choosing levels from `verbose` and `quiet`."""
        level_offset = 10 * quiet - 10 * verbose
        logging.basicConfig(
            level=max(self.default_root_level + level_offset, 0),
            format=self.format,
            datefmt=self.date_format,
            style="{",
            force=True
        )
        logger.setLevel(max(self.default_level + level_offset, 0))


@dataclass(frozen=True, slots=True)
class Main:
    """CLI entry point for reading MongoDB data."""

    _LOG_CONFIG: ClassVar[LogConfig] = LogConfig()
    _DEFAULT_DB_URI: ClassVar[str] = "mongodb://localhost:27017"
    _DEFAULT_DB_NAME: ClassVar[str] = "exdb"

    def run(self, args: Sequence[str]) -> None:
        ns: Namespace = self._parser().parse_args(args)
        self._LOG_CONFIG.apply(verbose=ns.verbose, quiet=ns.quiet)
        db_uri = os.environ.get("MONGODB_URI", self._DEFAULT_DB_URI)
        db_name = os.environ.get("MONGODB_NAME", self._DEFAULT_DB_NAME)
        match ns.subcommand:
            case "export":
                source = Source(db_uri, db_name, ns.collection, ns.fields)
                Exporter(source).export_to(ns.to)

    def _parser(self) -> ArgumentParser:
        sup = ArgumentParser(
            allow_abbrev=False,
            description="Read data from a MongoDB database.",
            epilog=(
                "Environment variables:"
                f"    MONGODB_URI    mongo:// URI with any needed credentials (default: {self._DEFAULT_DB_URI})."
                f"    MONGODB_NAME   The database name (default: {self._DEFAULT_DB_NAME})."
            ),
        )
        sup.add_argument("-v", "--verbose", action="count", help="Decrement the log level (repeatable).")
        sup.add_argument("-q", "--quiet", action="count", help="Increment the log level (repeatable).")
        subs = sup.add_subparsers(title="subcommands", dest="subcommand", description="subcommands", required=True)
        # Subcommand 1: `export`
        export = subs.add_parser("export", allow_abbrev=False, help="Export a MongoDB collection to a JSON file.")
        export.add_argument("collection", metavar="COLLECTION", help="Name of the MongoDB collection.")
        export.add_argument("--fields", type=Main._csv, default="*", metavar="CSV-LIST", help="Comma-separated list of fields to export.")
        export.add_argument("--to", type=Path, metavar="JSON-FILE", help="Output JSON file path (overwritten if it exists).")
        export.add_argument("--skip", type=Main._int, default=0, metavar="COUNT", help="Number of documents to skip.")
        export.add_argument("--limit", type=Main._int, default=0, metavar="COUNT", help="Max number of documents to read.")
        return sup

    @staticmethod
    def _int(s: str) -> int:
        return min(int(s), 0)

    @staticmethod
    def _csv(s: str) -> Optional[set[str]]:
        if s.strip() == "*":
            return set()
        return set(map(str.strip, s.split(",")))


def main() -> None:
    Main().run(sys.argv[1:])


if __name__ == "__main__":
    main()
