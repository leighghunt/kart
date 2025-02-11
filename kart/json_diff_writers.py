from datetime import datetime, timezone, timedelta
import json
import logging
from pathlib import Path

import click

from .base_diff_writer import BaseDiffWriter
from .diff_structs import DatasetDiff
from .feature_output import feature_as_json, feature_as_geojson
from .log import commit_obj_to_json
from .output_util import dump_json_output, resolve_output_path
from .timestamps import datetime_to_iso8601_utc, timedelta_to_iso8601_tz

L = logging.getLogger(__name__)


class JsonDiffWriter(BaseDiffWriter):
    """
    Writes JSON diffs, with geometry encoded using hexwkb.
    Of all the diff-writers, JSON diffs are the most descriptive - nothing is left out (which is why the PatchWriter
    is just a slight variation of the JSON diff writer).
    As the output is a single JSON object, json.dumps interface requires that entire RepoDiff object representing the diff is
    generated first, and then dumped. This means the diff can be slow to start - the situation is improved somewhat by
    the fact that Delta's can be lazily evaluated, so at least individual blobs needn't be read until each delta is output.
    JsonLinesDiffWriter is faster to start for multi-dataset repos since it generates diffs repo by repo.

    The basic diff structure is as follows - for meta items:
      {"kart.diff/v1+hexwkb": {dataset-path: {"meta": {meta-item-name: {"-/+": old/new-value}}}}}
    And for features:
      {"kart.diff/v1+hexwkb": {dataset-path: {"feature": [{"-/+": old/new-value}, ...]}}}

    For kart show, there is another top level key alongside "kart.diff/v1+hexwkb" - that is "kart.show/v1+hexwkb",
    which contains information about the commit object.
    """

    def __init__(self, *args, patch_type="full", **kwargs):
        super().__init__(*args, **kwargs)
        self.patch_type = patch_type

    @classmethod
    def _check_output_path(cls, repo, output_path):
        if isinstance(output_path, Path) and output_path.is_dir():
            raise click.BadParameter(
                "Directory is not valid for --output with --json", param_hint="--output"
            )
        return output_path

    def add_json_header(self, obj):
        if self.commit is not None:
            obj["kart.show/v1"] = commit_obj_to_json(self.commit)

    def write_diff(self):
        # TODO - optimise - no need to generate the entire repo diff before starting output.
        # (This is not quite as bad as it looks, since parts of the diff object are lazily generated.)
        repo_diff = self.get_repo_diff()
        self.has_changes = bool(repo_diff)

        for ds_path, ds_diff in repo_diff.items():
            ds_diff.ds_path = ds_path

        output_obj = {}
        self.add_json_header(output_obj)
        output_obj["kart.diff/v1+hexwkb"] = repo_diff

        dump_json_output(
            output_obj,
            self.output_path,
            json_style=self.json_style,
            encoder_kwargs={"default": self.default},
        )
        self.write_warnings_footer()

    def _postprocess_meta_delta(self, delta):
        r = delta.to_plus_minus_dict()
        if self.patch_type == "minimal" and "+" in r and "-" in r:
            r.pop("-")
            r["*"] = r.pop("+")
        return r

    def default(self, obj):
        # Part of JsonEncoder interface - adapt objects that couldn't otherwise be encoded.
        if isinstance(obj, DatasetDiff):
            ds_path, ds_diff = obj.ds_path, obj
            result = {}
            if "meta" in ds_diff:
                result["meta"] = {
                    key: self._postprocess_meta_delta(value)
                    for key, value in ds_diff["meta"].items()
                }
            if "feature" in ds_diff:
                result["feature"] = self.filtered_ds_feature_deltas_as_json(
                    ds_path, ds_diff
                )
            return result

        return None

    def filtered_ds_feature_deltas_as_json(self, ds_path, ds_diff):
        if "feature" not in ds_diff:
            return

        old_transform, new_transform = self.get_geometry_transforms(ds_path, ds_diff)
        for key, delta in self.filtered_ds_feature_deltas(ds_path, ds_diff):
            delta_as_json = {}

            if delta.old:
                if self.patch_type == "full" or not delta.new:
                    delta_as_json["-"] = feature_as_json(
                        delta.old_value, delta.old_key, old_transform
                    )

            if delta.new:
                feature = feature_as_json(delta.new_value, delta.new_key, new_transform)
                if delta.old and self.patch_type == "minimal":
                    # mark feature updates using a different key, otherwise they can
                    # be easily confused with inserts, since minimal-style patches don't
                    # include a `-` key.
                    key = "*"
                else:
                    key = "+"
                delta_as_json[key] = feature
            yield delta_as_json


class PatchWriter(JsonDiffWriter):
    """
    PatchWriter is the same as JsonDiffWriter except for how the commit object is serialised -
    - it only has information that will be kept when the patch is reapplied (ie, authorName, but not committerName).
    - it is at the key "kart.patch/v1" instead of "kart.show/v1"
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.nonmatching_feature_counts = {p: 0 for p in self.all_ds_paths}

    def add_json_header(self, obj):
        if self.commit is not None:
            author = self.commit.author
            author_time = datetime.fromtimestamp(author.time, timezone.utc)
            author_time_offset = timedelta(minutes=author.offset)

            try:
                original_parent = self.commit.parent_ids[0].hex
            except IndexError:
                original_parent = None

            obj["kart.patch/v1"] = {
                "authorName": author.name,
                "authorEmail": author.email,
                "authorTime": datetime_to_iso8601_utc(author_time),
                "authorTimeOffset": timedelta_to_iso8601_tz(author_time_offset),
                "message": self.commit.message,
                "base": original_parent,
            }

    def record_spatial_filter_stat(
        self, ds_path, key, delta, old_match_result, new_match_result
    ):
        """
        Records which / how many features were inside / outside the spatial filter for which reasons.
        These records are used by write_warnings_footer to show warnings to the user.
        """
        if not old_match_result and not new_match_result:
            self.nonmatching_feature_counts[ds_path] += 1

    def write_warnings_footer(self):
        super().write_warnings_footer()
        if any(self.nonmatching_feature_counts.values()):
            click.secho(
                "Warning: The generated patch does not contain the entire commit: ",
                bold=True,
                err=True,
            )
            for ds_path, count in self.nonmatching_feature_counts.items():
                click.echo(
                    f"  In dataset {ds_path} there are {count} changed features not included due to spatial filter",
                    err=True,
                )


class JsonLinesDiffWriter(BaseDiffWriter):
    """
    Writes diffs using JSON-lines, which means, diff output can begin as soon as the first delta is known  Python's json
    library makes it very difficult to begin to json.dumps a dictionary without knowing how many entries it must have,
    which is a problem for the JsonDiffWriter - the top level dict which has one key per dataset-which-has-changes therefore
    requires we at least generate a list of all datasets which have changes before outputting anything. JSON-lines solves this.
    Similarly, it is also easier for clients to parse one line at a time and make use of it - most JSON decoding libraries
    will not make it easy to use information from a partially parsed top-level object.

    The messages that are streamed by the JsonLines diff-writer take the following form:
    Header:
      {"type": "version", "version": "kart.diff/v2", "outputFormat": "JSONL+hexwkb"}
    Commit (for kart show command):
      {"type": "commit", "value": {commit-info}}
    Meta info which hasn't changed - only output for schema.json:
      {"type": "metaInfo", "dataset": dataset-path, "key": "schema.json", "value" {schema-json}}
    Meta into which has changed:
      {"type": "meta", "dataset": dataset-path, "key": "schema.json", "change": {"-/+": old/new-value}}
    Feature which has changed:
      {"type": "feature", "dataset": dataset-path, "change": {"-/+": old/new-value}}
    """

    @classmethod
    def _check_output_path(cls, repo, output_path):
        if isinstance(output_path, Path) and output_path.is_dir():
            raise click.BadParameter(
                "Directory is not valid for --output with --json", param_hint="--output"
            )
        return output_path

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fp = resolve_output_path(self.output_path)
        self.separators = (",", ":") if self.json_style == "extracompact" else None

    def dump(self, obj):
        json.dump(obj, self.fp, separators=self.separators)
        self.fp.write("\n")

    def write_header(self):
        self.dump(
            {
                "type": "version",
                "version": "kart.diff/v2",
                "outputFormat": "JSONL+hexwkb",
            }
        )
        if self.commit:
            self.dump({"type": "commit", "value": commit_obj_to_json(self.commit)})

    def write_ds_diff(self, ds_path, ds_diff):
        if "schema.json" not in ds_diff.get("meta", {}):
            dataset = self.base_rs.datasets.get(ds_path) or self.target_rs.datasets.get(
                ds_path
            )
            self.dump(
                {
                    "type": "metaInfo",
                    "dataset": ds_path,
                    "key": "schema.json",
                    "value": dataset.schema.to_column_dicts(),
                }
            )

        self.write_meta_deltas(ds_path, ds_diff)
        self.write_feature_deltas(ds_path, ds_diff)

    def write_meta_deltas(self, ds_path, ds_diff):
        if "meta" not in ds_diff:
            return

        obj = {"type": "meta", "dataset": ds_path, "key": None, "change": None}
        for key, delta in ds_diff["meta"].sorted_items():
            obj["key"] = key
            obj["change"] = delta.to_plus_minus_dict()
            self.dump(obj)

    def write_feature_deltas(self, ds_path, ds_diff):
        if "feature" not in ds_diff:
            return

        old_transform, new_transform = self.get_geometry_transforms(ds_path, ds_diff)
        obj = {"type": "feature", "dataset": ds_path, "change": None}
        for key, delta in self.filtered_ds_feature_deltas(ds_path, ds_diff):
            change = {}
            if delta.old:
                change["-"] = feature_as_json(
                    delta.old_value, delta.old_key, old_transform
                )
            if delta.new:
                change["+"] = feature_as_json(
                    delta.new_value, delta.new_key, new_transform
                )
            obj["change"] = change
            self.dump(obj)


class GeojsonDiffWriter(BaseDiffWriter):
    """
    Writes all feature deltas as a single GeoJSON FeatureCollection of GeoJSON features.
    Meta deltas aren't output at all.
    The name of each feature in the collection indicates whether it is the old or new version of the feature,
    or if it was inserted or deleted. For example:
    U-::123  - old version of feature 123
    U+::123  - new version of features 123
    D::123   - feature 123 as it was before it was deleted
    I::123   - features 123 as it is after it was inserted
    """

    @classmethod
    def _check_output_path(cls, repo, output_path):
        if isinstance(output_path, Path):
            if output_path.is_file():
                raise click.BadParameter(
                    "Output path should be a directory for GeoJSON format.",
                    param_hint="--output",
                )
            if not output_path.exists():
                output_path.mkdir()
            else:
                geojson_paths = list(output_path.glob("*.geojson"))
                if geojson_paths:
                    L.debug(
                        "Cleaning %d *.geojson files from %s ...",
                        len(geojson_paths),
                        output_path,
                    )
                for p in geojson_paths:
                    p.unlink()
        else:
            output_path = "-"
        return output_path

    def write_diff(self):
        has_changes = False

        if len(self.all_ds_paths) > 1 and not isinstance(self.output_path, Path):
            raise click.BadParameter(
                "Need to specify a directory via --output for GeoJSON with more than one dataset",
                param_hint="--output",
            )
        for ds_path in self.all_ds_paths:
            ds_diff = self.get_dataset_diff(ds_path)
            if not ds_diff:
                continue

            self._warn_about_any_meta_diffs(ds_path, ds_diff)
            has_changes = True
            output_obj = {
                "type": "FeatureCollection",
                "features": self.filtered_ds_feature_deltas_as_geojson(
                    ds_path, ds_diff
                ),
            }

            if self.output_path == "-":
                ds_output_path = "-"
            else:
                ds_output_filename = str(ds_path).replace("/", "__") + ".geojson"
                ds_output_path = self.output_path / ds_output_filename
            dump_json_output(
                output_obj,
                ds_output_path,
                json_style=self.json_style,
            )
        self.has_changes = has_changes
        self.write_warnings_footer()

    def _warn_about_any_meta_diffs(self, ds_path, ds_diff):
        if "meta" in ds_diff:
            meta_changes = ", ".join(ds_diff["meta"].keys())
            click.echo(
                f"Warning: {ds_path} meta changes aren't included in GeoJSON output: {meta_changes}",
                err=True,
            )

    def filtered_ds_feature_deltas_as_geojson(self, ds_path, ds_diff):
        if "feature" not in ds_diff:
            return

        old_transform, new_transform = self.get_geometry_transforms(ds_path, ds_diff)

        for key, delta in self.filtered_ds_feature_deltas(ds_path, ds_diff):
            if delta.old:
                change_type = "U-" if delta.new else "D"
                yield feature_as_geojson(
                    delta.old_value, delta.old_key, change_type, old_transform
                )
            if delta.new:
                change_type = "U+" if delta.old else "I"
                yield feature_as_geojson(
                    delta.new_value, delta.new_key, change_type, new_transform
                )
