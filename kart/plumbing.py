import sys

import click
import msgpack
import pygit2

from .cli_util import add_help_subcommand
from .geometry import Geometry
from .repo import KartRepoState
from .serialise_util import msg_unpack


@add_help_subcommand
@click.group()
@click.pass_context
def plumbing(ctx, **kwargs):
    """
    Internal data format processing tools.
    """


@plumbing.command()
@click.pass_context
@click.option(
    "--output-format",
    "-o",
    type=click.Choice(["envelope", "hexwkb"]),
    default="hexwkb",
    help="Output format",
)
@click.argument("blob_oid")
def feature_geometry(ctx, *, blob_oid, output_format, **kwargs):
    """
    Given a Blob OID, return a geometry in the specified output format:
    * envelope: minx,maxx,miny,maxy or an empty string for an empty geometry
    * hexwkb: hex-encoded ISO WKB

    If there's no geometry or it's null, returns an empty string.

    Be aware the geometry coordinates are in the native CRS.

    Specify blob_oid=- to process multiple line-delimited values from stdin.
    """
    repo = ctx.obj.get_repo(allowed_states=KartRepoState.ALL_STATES)

    is_stream = blob_oid == "-"
    if is_stream:
        sys.stdout.reconfigure(line_buffering=True)
        blob_seq = (line.rstrip() for line in sys.stdin)
    else:
        blob_seq = [blob_oid]

    def _err(msg):
        if is_stream:
            print(f"E: {msg}")
        else:
            raise click.BadParameter(msg, param_hint="blob_oid")

    for blob_oid in blob_seq:
        try:
            obj = repo[blob_oid]
            if not isinstance(obj, pygit2.Blob):
                raise KeyError(blob_oid)
        except (KeyError, ValueError):
            _err("invalid-oid")
            continue

        try:
            legend_hash, non_pk_values = msg_unpack(obj)
        except msgpack.UnpackException:
            _err("invalid-feature")
            continue

        for field in non_pk_values:
            if isinstance(field, Geometry):
                if output_format == "hexwkb":
                    print(field.to_hex_wkb())
                elif output_format == "envelope":
                    e = field.envelope()
                    if e:
                        print("{},{},{},{}".format(*e))
                    else:
                        print()
                break
        else:
            print()
