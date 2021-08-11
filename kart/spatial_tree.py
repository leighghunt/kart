import logging
import time

import click
import pywraps2 as s2
from osgeo import osr, ogr


from . import structure
from .geometry import geom_envelope, gpkg_geom_to_ogr
from .crs_util import make_crs


L = logging.getLogger("sno.spatial_tree")


S2_INDEX_REF = "refs/spatial/s2"
S2_MAX_CELLS_INDEX = 8
S2_MAX_CELLS_QUERY = 25
S2_MAX_LEVEL = 15


def build_spatial_tree(dataset):
    if not dataset.has_geometry:
        raise ValueError("No geometry to index")

    # Find the transform to reproject this dataset into the target CRS.
    src_crs = make_crs(dataset.get_crs_definition())
    if src_crs.IsGeographic():
        crs_transform = None
        L.info(f"no CRSTransform: {src_crs.GetAuthorityCode(None)}")
    else:
        target_crs = src_crs.CloneGeogCS()
        crs_transform = osr.CoordinateTransformation(src_crs, target_crs)
        L.info(
            f"CRSTransform: {src_crs.GetAuthorityCode(None)} -> {target_crs.GetAuthorityCode(None)}"
        )

    geometry_type = dataset.schema.geometry_columns[0].extra_type_info.get(
        "geometryType", "GEOMETRY"
    )
    is_point = geometry_type == "POINT"

    L.info(f"GeometryType: {geometry_type}")

    s2_coverer = s2.S2RegionCoverer()
    s2_coverer.set_max_cells(S2_MAX_CELLS_INDEX)
    s2_coverer.set_max_level(S2_MAX_LEVEL)

    t0 = time.monotonic()
    for i, (feature, blob) in enumerate(dataset.features_plus_blobs()):
        geom = feature[dataset.geom_column_name]

        if geom is None:
            continue

        # L.debug(i, blob.hex)
        # L.debug(gpkg_geom_to_ogr(geom).ExportToWkt())

        if is_point:
            g = gpkg_geom_to_ogr(geom)

            if crs_transform:
                g.Transform(crs_transform)

            p_dest = g.GetPoint()[:2]
            # L.debug(p_dest)
            s2_ll = s2.S2LatLng.FromDegrees(p_dest[1], p_dest[0]).Normalized()
            s2_cell_ids = (s2.S2CellId(s2_ll.ToPoint()),)

        else:
            e = geom_envelope(geom)
            if e is None:
                continue  # empty

            # L.debug(e)
            sw_src, ne_src = (e[0::2], e[1::2])
            s2_ll = []
            # L.debug(sw_src, ne_src)

            for p_src in (sw_src, ne_src):
                g = ogr.Geometry(ogr.wkbPoint)
                g.AddPoint(*p_src)
                if crs_transform:
                    g.Transform(crs_transform)
                p_dest = g.GetPoint()[:2]
                # L.debug(p_dest)
                s2_ll.append(s2.S2LatLng.FromDegrees(p_dest[1], p_dest[0]).Normalized())

            s2_llrect = s2.S2LatLngRect.FromPointPair(*s2_ll)
            s2_cell_ids = s2_coverer.GetCovering(s2_llrect)

        print("{} {}".format(blob.hex, " ".join(c.ToToken() for c in s2_cell_ids)))

    t1 = time.monotonic()
    L.info(f" {i+1} features")
    t_rate = (i + 1) / (t1 - t0)
    L.info(f" {t_rate:.1f} features/s")


@click.command(
    "spatial-tree", hidden=True, context_settings=dict(ignore_unknown_options=True)
)
@click.pass_context
@click.argument(
    "command",
    type=click.Choice(("index", "filter")),
    required=True,
)
@click.argument("params", nargs=-1, required=False)
def spatial_tree(ctx, command, params):
    """
    Find features in a Dataset

    WARNING: Spatial Trees are a proof of concept.
    Significantly, indexes don't update when the repo changes in any way.
    """
    repo = ctx.obj.repo
    rs = repo.structure("HEAD")

    if command == "index":
        t0 = time.monotonic()
        for dataset in rs.datasets:
            L.info(dataset)

            if not dataset.has_geometry:
                L.info("  has no geometry, skipping")
                continue

            if not dataset.get_crs_definition():
                L.info("  has no crs, skipping")
                continue

            build_spatial_tree(dataset)

            t1 = time.monotonic()
            L.info("Indexed %s in %0.3fs", dataset, t1 - t0)

    # if command == "filter":
    #     USAGE = 'filter W,S,E,N [--write]'
    #     try:
    #         w, s, e, n = map(float, re.split(r',\s*', params[0]))
    #     except (IndexError, TypeError):
    #         raise click.BadParameter(USAGE)

    #     filter_specs = get_filter_spec((w, s, e, n))

    #     if len(params) > 1 and params[1] == '--write':
    #         blob_content = '\n'.join(filter_specs) + '\n'
    #         blob_oid = repo.create_blob(blob_content.encode('utf-8'))
    #         print(f"\nFilter Spec written to repo as: {blob_oid.hex}")

    else:
        raise NotImplementedError(f"Unknown command: {command}")

    # L.debug("Results in %0.3fs", t1 - t0)
    # t2 = time.monotonic()
    # dump_json_output(results, sys.stdout)
    # L.debug("Output in %0.3fs", time.monotonic() - t2)
