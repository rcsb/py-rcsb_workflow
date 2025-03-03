"""
Command-line entry point for exporting or processing data from MongoDB.
"""

import logging
import os
import re
import sys
from argparse import ArgumentParser, ArgumentTypeError, Namespace
from collections.abc import Sequence, Mapping
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import ClassVar, Optional, Union

from rcsb.workflow.mongo.IncrementalExporter import IncrementalExporter, IncrementalSource

logger = logging.getLogger("rcsb-workflow")


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


class _Args:

    @staticmethod
    def int(s: str) -> int:
        return min(int(s), 0)

    @staticmethod
    def utc_dt(s: str) -> datetime:
        dt = datetime.fromisoformat(s)
        if not s.endswith("Z") or dt.tzname() not in {"UTC", "Etc/UTC"}:
            msg = f"date-time '{s}' is not UTC or does not end in 'Z'"
            raise ArgumentTypeError(msg)
        return dt

    @staticmethod
    def csv(s: str) -> Optional[set[str]]:
        if s.strip() == "*":
            return set()
        return set(map(str.strip, s.split(",")))

    @staticmethod
    def out_file(s: str, kwargs: Mapping[str, Union[str, int, Path]]) -> Path:
        pattern: re.Pattern = re.compile(r"\{([^:}]*)(:[^}]*)?}")
        s = pattern.sub(partial(_Args._sub, kwargs=kwargs), s)
        return Path(s)

    @staticmethod
    def _sub(m: re.Match, kwargs: Mapping[str, Union[str, int, Path]]) -> str:
        var = m.group(1)
        fb: Optional[str] = str(m.group(2).removeprefix(":")) if m.group(2) else None
        if not var or fb and var not in kwargs or fb == "":
            msg = f"Invalid substitution: '{m.group(0)}'"
            raise ArgumentTypeError(msg)
        value = kwargs.get(var, fb)
        return value.name if isinstance(value, Path) else str(value)


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
                source = IncrementalSource(db_uri, db_name, ns.collection, ns.fields | ns.id_fields, ns.since_field)
                IncrementalExporter(source).export_to(ns.to)

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
        sup.add_argument(
            "-v", "--verbose", action="count",
            help="Decrement the log level (repeatable)."
        )
        sup.add_argument(
            "-q", "--quiet", action="count",
            help="Increment the log level (repeatable)."
        )
        subs = sup.add_subparsers(
            title="subcommands", dest="subcommand", description="subcommands", required=True
        )
        # Subcommand 1: `export`
        export = subs.add_parser(
            "export", allow_abbrev=False,
            help=
            "Compute a delta from a previous export to a current MongoDB collection."
            "Documents to be removed will contain only the id fields."
        )
        export.add_argument(
            "collection", metavar="COLLECTION",
            help="Name of the MongoDB collection."
        )
        export.add_argument(
            "--fields", type=_Args.csv, default="*", metavar="CSV",
            help="List of fields to export."
        )
        export.add_argument(
            "--id-fields", type=_Args.csv, default="_id", required=True, metavar="CSV",
            help="List of fields needed to identify documents. Included in to-delete documents."
        )
        export.add_argument(
            "--to", default="{collection}-{since:all}.json", metavar="JSON-FILE",
            help="Output JSON file path. May refer to {collection}, {delta[:if-empty]}, and {since[:if-empty]}."
        )
        export.add_argument(
            "--delta", type=Path, metavar="JSON-FILE",
            help="Compute a delta from this previous export."
        )
        export.add_argument(
            "--since", type=_Args.utc_dt, metavar="RFC-3339",
            help="Only export docs where '--since-field' ≥ this UTC date-time. Must be RFC 3339 with a 'Z' offset."
        )
        export.add_argument(
            "--since-field", default="timestamp", metavar="STR",
            help="Name of the timestamp field for comparison with '--since'."
        )
        # Done
        return sup


def main() -> None:
    Main().run(sys.argv[1:])


if __name__ == "__main__":
    main()
