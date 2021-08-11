#!/usr/bin/env python3

import argparse
import logging
import os
import re
import sqlite3
import sys

import pywraps2 as s2


L = logging.getLogger()

S2_MAX_CELLS_QUERY = 25
S2_MAX_LEVEL = 15

# DB Structure
SQL_DB_FILE = "s2_index.db"
# CREATE TABLE blobs (
#   blob_id TEXT NOT NULL PRIMARY KEY
# );
# CREATE TABLE blob_cells (
#   blob_rowid INTEGER NOT NULL,
#   cell_token TEXT NOT NULL,
#   PRIMARY KEY(blob_rowid, cell_token),
#   FOREIGN KEY(blob_rowid) REFERENCES blobs(rowid)
# );

GIT_FILTER_UNDECIDED = -1
GIT_FILTER_NOT_MATCHED = 0
GIT_FILTER_MATCH = 1
GIT_FILTER_MATCH_RECURSIVE = 2

re_filter = re.compile(r"(?P<otype>blob|tree) (?P<oid>[0-9a-f]{40}) (?P<opath>.+)?$")
re_sno_feature_path = re.compile(r"/\.sno-dataset/feature/")
re_sno_meta_path = re.compile(r"/\.sno-dataset/meta/")


def git_filter(rect, **options):
    indexer = s2.S2RegionTermIndexer()
    indexer.set_max_cells(S2_MAX_CELLS_QUERY)
    indexer.set_max_level(S2_MAX_LEVEL)

    query_tokens = tuple(indexer.GetQueryTerms(rect, ""))
    L.info("Query terms for %s: %s", rect, query_tokens)

    db = sqlite3.connect(f"file:{SQL_DB_FILE}?mode=ro", uri=True)

    sql_lookup = (
        """
        SELECT EXISTS(
            SELECT 1
            FROM blobs
                INNER JOIN blob_cells ON (blobs.rowid=blob_cells.blob_rowid)
            WHERE
                blobs.blob_id=?
                AND blob_cells.cell_token IN ({})
        );"""
    ).format(",".join(["?"] * len(query_tokens)))

    try:
        for line in sys.stdin:
            m = re_filter.match(line)
            if not m:
                L.error("Invalid filter line: %s", repr(line))
                return 1

            otype, oid, opath = m.groups(default="")
            if otype == "blob" and re_sno_feature_path.search(opath):
                r = db.execute(sql_lookup, (oid,) + query_tokens).fetchone()  # (1,)
                if r[0]:
                    print(GIT_FILTER_MATCH)
                else:
                    print(GIT_FILTER_NOT_MATCHED)
                continue

            elif otype == "tree" and re_sno_meta_path.search(opath):
                print(GIT_FILTER_MATCH_RECURSIVE)
                continue

            print(GIT_FILTER_MATCH)
    except KeyboardInterrupt:
        return 2


def write_index(source, **options):
    db = sqlite3.connect(f"file:{SQL_DB_FILE}", uri=True)
    with db:
        cursor = db.cursor()
        cursor.execute("DROP TABLE IF EXISTS blob_cells;")
        cursor.execute("DROP TABLE IF EXISTS blobs;")
        cursor.execute("CREATE TABLE blobs (blob_id BLOB NOT NULL PRIMARY KEY);")
        cursor.execute(
            """
            CREATE TABLE blob_cells (
              blob_rowid INTEGER NOT NULL,
              cell_token TEXT NOT NULL,
              PRIMARY KEY(blob_rowid, cell_token),
              FOREIGN KEY(blob_rowid) REFERENCES blobs(rowid)
            );"""
        )

        for line in source:
            line = line[:-1]
            if not line or line.startswith("#"):
                continue

            blob_oid, *cell_tokens = line.split()
            assert len(blob_oid) == 40, f"Invalid blob oid: {blob_oid}"
            assert len(cell_tokens), f"No cell tokens for: {blob_oid}"
            assert all(cell_tokens), f"Empty cell tokens for: {blob_oid}"

            try:
                cursor.execute(
                    "INSERT INTO blobs (blob_id) VALUES (?);",
                    (bytes.fromhex(blob_oid),),
                )
                blob_rowid = cursor.lastrowid
                cursor.executemany(
                    "INSERT INTO blob_cells (blob_rowid, cell_token) VALUES (?, ?);",
                    ((blob_rowid, c) for c in cell_tokens),
                )
            except (sqlite3.Error, OverflowError):
                L.error("Error processing %s: %s", blob_oid, cell_tokens)
                raise

    sql_count = """
        SELECT
            (SELECT COUNT(*) FROM blob_cells),
            (SELECT COUNT(*) FROM blobs);
    """
    c_total, c_blobs = db.execute(sql_count).fetchone()
    print(f"Wrote blob cell map: {c_blobs} blobs, {c_total} total entries")


def dump_index(**options):
    db = sqlite3.connect(f"file:{SQL_DB_FILE}?mode=ro", uri=True)
    sql_dump = """
        SELECT HEX(blobs.blob_id), group_concat(blob_cells.cell_token, ' ')
        FROM blobs
            LEFT OUTER JOIN blob_cells ON (blobs.rowid=blob_cells.blob_rowid)
        GROUP BY blobs.blob_id
        ORDER BY blobs.blob_id;
    """
    for row in db.execute(sql_dump):
        print("{}\t{}".format(*row))


def arg_ll_rect(arg_value):
    """ Validate a LatLngRect expression: '<lat_s>,<lng_w>,<lat_n>,<lng_e>' """
    try:
        bounds = tuple(float(v) for v in arg_value.split(","))
        if not len(bounds) == 4:
            raise TypeError()
    except TypeError:
        raise argparse.ArgumentTypeError(
            "Invalid bounds format. Expected: <lat_s>,<lng_w>,<lat_n>,<lng_e>"
        )

    sw = s2.S2LatLng.FromDegrees(*bounds[:2])
    ne = s2.S2LatLng.FromDegrees(*bounds[2:])

    if not (sw.is_valid() and ne.is_valid()):
        raise argparse.ArgumentTypeError("Invalid latitude/longitude value")

    sw = sw.Normalized()
    ne = ne.Normalized()
    L.debug("SW: %s NE: %s", sw, ne)

    return s2.S2LatLngRect.FromPointPair(sw, ne)


def main():
    logging.basicConfig(
        format="%(filename)s: %(levelname)s %(message)s", level=logging.DEBUG
    )

    # hackarooney
    args = sys.argv[1:]
    if (not len(args)) or (args[0] not in ("write", "dump", "-h", "--help")):
        args[:0] = ["filter"]

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)

    # default "filter" command
    parser_filter = subparsers.add_parser("filter", help="Git filtering (the default)")
    parser_filter.add_argument(
        "rect",
        type=arg_ll_rect,
        help="LatLngRect filter: '<lat_s>,<lng_w>,<lat_n>,<lng_e>'",
    )
    parser_filter.set_defaults(func=git_filter, needs_db=True)

    # "write" command
    parser_write = subparsers.add_parser("write", help="Write a new spatial index")
    parser_write.add_argument(
        "source",
        type=argparse.FileType("rt", encoding="UTF-8"),
        nargs="?",
        default=sys.stdin,
        help="filename to read from, or - for stdin",
    )
    parser_write.set_defaults(func=write_index)

    # "dump" command
    parser_dump = subparsers.add_parser("dump", help="Print the spatial index")
    parser_dump.set_defaults(func=dump_index, needs_db=True)

    options = parser.parse_args(args)

    if getattr(options, "needs_db", False):
        if not os.path.isfile(SQL_DB_FILE):
            L.error(
                f"Couldn't find spatial index: {SQL_DB_FILE} (cwd=%s)",
                os.path.abspath(os.curdir),
            )
            return 1

    return options.func(**vars(options))


if __name__ == "__main__":
    sys.exit(main() or 0)
