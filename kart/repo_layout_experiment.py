import csv
import os
import re
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path

import click
import pygit2

from .cli_util import tool_environment
from .fast_import import _git_fast_import
from .serialise_util import msg_pack, b64encode_str, hexhash


NUM_COMMITS_PER_REPO = 100
BLOB_SIZE = 150


ALL_STRATEGIES = (
    "two_layer_b64_hashed_x256",
    "three_layer_b64_hashed_x256",
    "three_layer_b64_hashed_x128",
    "two_layer_b64_hashed_x128",
    "two_layer_b64_hashed_x64",
    "three_layer_b64_hashed_x64",
    "four_layer_b64_hashed_x64",
    "three_layer_b64_hashed_x32",
    "four_layer_b64_hashed_x32",
    "five_layer_b64_hashed_x32",
    "two_layer_nonhashed_x128",
    "three_layer_nonhashed_x128",
    "two_layer_nonhashed_x256",
    "three_layer_nonhashed_x256",
    "two_layer_nonhashed_x64",
    "three_layer_nonhashed_x64",
    "four_layer_nonhashed_x64",
    "three_layer_nonhashed_x32",
    "four_layer_nonhashed_x32",
    "five_layer_nonhashed_x32",
)


def serial_pks():
    i = 0
    while True:
        yield i
        i += 1


class RepoGenerator:
    def __init__(self, dest_dir, feature_path_generator, pk_generator):
        self.feature_path_generator = feature_path_generator
        self.repo = pygit2.Repository(str(dest_dir))
        self._git_fast_import = _git_fast_import(self.repo, "--done", "--depth=0")
        self.pk_generator = pk_generator

    def __enter__(self):
        self.p = self._git_fast_import.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, trace):
        # this commits, if no exception was raised
        self._git_fast_import.__exit__(exc_type, exc_value, trace)

    @contextmanager
    def make_commit(self):
        self.p.stdin.write(
            b"commit refs/heads/main\n"
            b"author testuser <craig.destigter+testuser@koordinates.com> 1000000000 +1200\n"
            b"committer testuser <craig.destigter+testuser@koordinates.com> 1000000000 +1200\n"
            b"data 11\ntest commit\n"
        )
        # self.p.stdin.write(f"")
        # if not self.repo.head_is_unborn:
        #     if self.last_commit_mark:
        #         self.p.stdin.write(f"from {self.last_commit_mark}\n".encode())
        #     else:
        #         self.p.stdin.write(f"from {self.repo.head.target}\n".encode())
        yield

    def insert(self, blob_data):
        """
        Creates a blob at a random path.
        Might replace an existing blob if there's one there.
        """
        pk = next(self.pk_generator)
        path = self.feature_path_generator(pk)
        return self.add_blob(path, blob_data)

    def add_blob(self, path, blob_data):
        self.p.stdin.write(
            f"M 644 inline {path}\ndata {len(blob_data)}\n".encode("utf8")
        )
        self.p.stdin.write(blob_data)
        self.p.stdin.write(b"\n")
        return path

    def delete(self, path):
        """
        Deletes one random existing blob from the previous revision.
        """
        self.p.stdin.write(f"D {path}\n".encode("utf8"))


def two_layer_b64_hashed_x256(pk):
    """
    The current sno/kart path encoding:
    * msgpack ID
    * then base64-encode it
    * then chop into two tree levels and a filename
    * with branching factor of 256
    """
    packed_pk = msg_pack((pk,))
    pk_hash = hexhash(packed_pk)
    filename = b64encode_str(packed_pk)
    return f"mydatasetname/.sno-dataset/{pk_hash[:2]}/{pk_hash[2:4]}/{filename}"


def three_layer_b64_hashed_x256(pk):
    """
    Almost the current sno/kart path encoding, but add another tree level
    """
    packed_pk = msg_pack((pk,))
    pk_hash = hexhash(packed_pk)
    filename = b64encode_str(packed_pk)
    return f"mydatasetname/.sno-dataset/{pk_hash[:2]}/{pk_hash[2:4]}/{pk_hash[4:6]}/{filename}"


def _branching_factor_128(two_hex_bytes):
    i = int(two_hex_bytes, 16)
    return f"{i % 128:02x}"


def two_layer_b64_hashed_x128(pk):
    """
    Almost the current sno/kart path encoding, but:
    * use branching factor 128 instead of 256
    """
    packed_pk = msg_pack((pk,))
    pk_hash = hexhash(packed_pk)
    filename = b64encode_str(packed_pk)

    return (
        f"mydatasetname/.sno-dataset/"
        f"{_branching_factor_128(pk_hash[:2])}/"
        f"{_branching_factor_128(pk_hash[2:4])}/{filename}"
    )


def three_layer_b64_hashed_x128(pk):
    """
    Almost the current sno/kart path encoding, but:
    * add another tree level
    * use branching factor 128 instead of 256
    """
    packed_pk = msg_pack((pk,))
    pk_hash = hexhash(packed_pk)
    filename = b64encode_str(packed_pk)

    return (
        f"mydatasetname/.sno-dataset/"
        f"{_branching_factor_128(pk_hash[:2])}/"
        f"{_branching_factor_128(pk_hash[2:4])}/"
        f"{_branching_factor_128(pk_hash[4:6])}/{filename}"
    )


def _branching_factor_64(two_hex_bytes):
    i = int(two_hex_bytes, 16)
    return f"{i % 64:02x}"


def two_layer_b64_hashed_x64(pk):
    """
    Almost the current sno/kart path encoding, but:
    * use branching factor 64 instead of 256
    """
    packed_pk = msg_pack((pk,))
    pk_hash = hexhash(packed_pk)
    filename = b64encode_str(packed_pk)

    return (
        f"mydatasetname/.sno-dataset/"
        f"{_branching_factor_64(pk_hash[:2])}/"
        f"{_branching_factor_64(pk_hash[2:4])}/{filename}"
    )


def three_layer_b64_hashed_x64(pk):
    """
    Almost the current sno/kart path encoding, but:
    * add another tree level
    * use branching factor 64 instead of 256
    """
    packed_pk = msg_pack((pk,))
    pk_hash = hexhash(packed_pk)
    filename = b64encode_str(packed_pk)

    return (
        f"mydatasetname/.sno-dataset/"
        f"{_branching_factor_64(pk_hash[:2])}/"
        f"{_branching_factor_64(pk_hash[2:4])}/"
        f"{_branching_factor_64(pk_hash[4:6])}/{filename}"
    )


def four_layer_b64_hashed_x64(pk):
    """
    Almost the current sno/kart path encoding, but:
    * add two more tree levels
    * use branching factor 64 instead of 256
    """
    packed_pk = msg_pack((pk,))
    pk_hash = hexhash(packed_pk)
    filename = b64encode_str(packed_pk)

    return (
        f"mydatasetname/.sno-dataset/"
        f"{_branching_factor_64(pk_hash[:2])}/"
        f"{_branching_factor_64(pk_hash[2:4])}/"
        f"{_branching_factor_64(pk_hash[4:6])}/"
        f"{_branching_factor_64(pk_hash[6:8])}/{filename}"
    )


def _branching_factor_32(two_hex_bytes):
    i = int(two_hex_bytes, 16)
    return f"{i % 32:02x}"


def three_layer_b64_hashed_x32(pk):
    """
    Almost the current sno/kart path encoding, but:
    * add another tree level
    * use branching factor 32 instead of 256
    """
    packed_pk = msg_pack((pk,))
    pk_hash = hexhash(packed_pk)
    filename = b64encode_str(packed_pk)

    return (
        f"mydatasetname/.sno-dataset/"
        f"{_branching_factor_32(pk_hash[:2])}/"
        f"{_branching_factor_32(pk_hash[2:4])}/"
        f"{_branching_factor_32(pk_hash[4:6])}/{filename}"
    )


def four_layer_b64_hashed_x32(pk):
    """
    Almost the current sno/kart path encoding, but:
    * add two more tree levels
    * use branching factor 32 instead of 256
    """
    packed_pk = msg_pack((pk,))
    pk_hash = hexhash(packed_pk)
    filename = b64encode_str(packed_pk)

    return (
        f"mydatasetname/.sno-dataset/"
        f"{_branching_factor_32(pk_hash[:2])}/"
        f"{_branching_factor_32(pk_hash[2:4])}/"
        f"{_branching_factor_32(pk_hash[4:6])}/"
        f"{_branching_factor_32(pk_hash[6:8])}/{filename}"
    )


def five_layer_b64_hashed_x32(pk):
    """
    Almost the current sno/kart path encoding, but:
    * add 3 more tree levels
    * use branching factor 32 instead of 256
    """
    packed_pk = msg_pack((pk,))
    pk_hash = hexhash(packed_pk)
    filename = b64encode_str(packed_pk)

    return (
        f"mydatasetname/.sno-dataset/"
        f"{_branching_factor_32(pk_hash[:2])}/"
        f"{_branching_factor_32(pk_hash[2:4])}/"
        f"{_branching_factor_32(pk_hash[4:6])}/"
        f"{_branching_factor_32(pk_hash[6:8])}/"
        f"{_branching_factor_32(pk_hash[8:10])}/{filename}"
    )


def two_layer_nonhashed_x256(pk):
    """
    Don't hash the PK at all, use it as it is, with a branchingfactor=256 and depth=2
    """
    return (
        f"mydatasetname/.sno-dataset/{(pk%16777216)//65536}/{(pk%65536)//256}/{pk%256}"
    )


def three_layer_nonhashed_x256(pk):
    """
    Don't hash the PK at all, use it as it is, with a branchingfactor=256 and depth=3
    """
    return f"mydatasetname/.sno-dataset/{(pk%4294967296)//16777216}/{(pk%16777216)//65536}/{(pk%65536)//256}/{pk%256}"


def two_layer_nonhashed_x128(pk):
    """
    Don't hash the PK at all, use it as it is, with a branchingfactor=128 and depth=2
    """
    return (
        f"mydatasetname/.sno-dataset/{(pk%2097152)//16384}/{(pk%16384)//128}/{pk%128}"
    )


def three_layer_nonhashed_x128(pk):
    """
    Don't hash the PK at all, use it as it is, with a branchingfactor=128 and depth=3
    """
    return f"mydatasetname/.sno-dataset/{(pk%268435456)//2097152}/{(pk%2097152)//16384}/{(pk%16384)//128}/{pk%128}"


def two_layer_nonhashed_x64(pk):
    """
    Don't hash the PK at all, use it as it is, with a branchingfactor=64 and depth=2
    """
    return f"mydatasetname/.sno-dataset/{(pk%262144)//4096}/{(pk%4096)//64}/{pk%64}"


def three_layer_nonhashed_x64(pk):
    """
    Don't hash the PK at all, use it as it is, with a branchingfactor=64 and depth=3
    """
    return f"mydatasetname/.sno-dataset/{(pk%16777216)//262144}/{(pk%262144)//4096}/{(pk%4096)//64}/{pk%64}"


def four_layer_nonhashed_x64(pk):
    """
    Don't hash the PK at all, use it as it is, with a branchingfactor=64 and depth=4
    """
    return f"mydatasetname/.sno-dataset/{(pk%1073741824)//16777216}/{(pk%16777216)//262144}/{(pk%262144)//4096}/{(pk%4096)//64}/{pk%64}"


def three_layer_nonhashed_x32(pk):
    """
    Don't hash the PK at all, use it as it is, with a branchingfactor=32 and depth=3
    """
    return f"mydatasetname/.sno-dataset/{(pk%1048576)//32768}/{(pk%32768)//1024}/{(pk%1024)//32}/{pk%32}"


def four_layer_nonhashed_x32(pk):
    """
    Don't hash the PK at all, use it as it is, with a branchingfactor=32 and depth=4
    """
    return f"mydatasetname/.sno-dataset/{(pk%33554432)//1048576}/{(pk%1048576)//32768}/{(pk%32768)//1024}/{(pk%1024)//32}/{pk%32}"


def five_layer_nonhashed_x32(pk):
    """
    Don't hash the PK at all, use it as it is, with a branchingfactor=32 and depth=5
    """
    return f"mydatasetname/.sno-dataset/{(pk%1073741824)//33554432}/{(pk%33554432)//1048576}/{(pk%1048576)//32768}/{(pk%32768)//1024}/{(pk%1024)//32}/{pk%32}"


@click.command(name="generate-layouts")
@click.pass_context
@click.argument(
    "output_dir", type=click.Path(exists=True, file_okay=False, dir_okay=True)
)
def generate_layouts(ctx, *, output_dir, **kwargs):
    """
    Generates repos in a bunch of different proposed layouts
    """
    output_dir = Path(output_dir)
    for feature_path_strategy in ALL_STRATEGIES:
        feature_path_generator = globals()[feature_path_strategy]
        for n_features in (
            5_000,
            50_000,
            500_000,
            5_000_000,
            # 25_000_000,
            # 50_000000,
            # 500_000000,
        ):
            dest_dir = output_dir / f"{feature_path_strategy}-{n_features}"
            try:
                dest_dir.mkdir()
            except FileExistsError:
                pass
            else:
                subprocess.check_call(
                    ["git", "init", "--bare", str(dest_dir)], env=tool_environment()
                )
            with RepoGenerator(dest_dir, feature_path_generator, serial_pks()) as gen:
                # now generate 100 commits
                all_paths = set()
                for i in range(NUM_COMMITS_PER_REPO):
                    if not i % 5:
                        print(f"commit {i}/{NUM_COMMITS_PER_REPO}...")
                    with gen.make_commit():
                        # do 50% of the features in the first commit, to simulate 'initial import' commit
                        if i == 0:
                            inserts = n_features // 2
                        else:
                            # divide the other 50% of features up across all the rest of the commits
                            increased_features_per_commit = (
                                n_features // 2 // NUM_COMMITS_PER_REPO
                            )
                            deletes = increased_features_per_commit // 2
                            inserts = increased_features_per_commit + deletes

                            for j in range(deletes):
                                gen.delete(all_paths.pop())
                        for j in range(inserts):
                            all_paths.add(gen.insert(os.urandom(BLOB_SIZE)))
                print("done.")


@click.command(name="inspect-repos")
@click.pass_context
@click.argument(
    "repos",
    type=click.Path(exists=True, file_okay=False, dir_okay=True),
    nargs=-1,
    required=True,
)
def inspect_repos(ctx, *, repos, **kwargs):
    """
    Generates repos in a bunch of different proposed layouts
    """
    w = csv.writer(sys.stdout)
    w.writerow(
        (
            "test name",
            "hashed?",
            "branching factor",
            "tree levels",
            "number of blobs",
            "number of trees",
            "total size of trees (GB)",
            "total repo size (GB)",
            "compressed repo size (GB)",
            "total size of compressed trees (GB)",
        )
    )
    # sort by numfeatures
    for path in sorted(repos, key=lambda p: int(p.split("-")[-1])):
        path = Path(path)
        name = path.name
        is_hashed = "nonhashed" not in name
        branching_factor = int(re.search(r"_x(\d+)", name).group(1))
        repo = pygit2.Repository(str(path))
        ds_tree = repo.head.peel(pygit2.Tree) / "mydatasetname/.sno-dataset"
        actual_branching_factor = len(list(ds_tree))
        assert actual_branching_factor <= branching_factor, actual_branching_factor

        num_levels = re.search(r"(.*)_layer_", name).group(1)
        num_levels = ["zero", "one", "two", "three", "four", "five"].index(num_levels)
        num_trees = 0
        num_blobs = 0
        total_tree_size = 0
        total_compressed_tree_size = 0
        uncompressed_repo_size = 0
        all_object_types = subprocess.check_output(
            [
                "git",
                "-C",
                str(path),
                "cat-file",
                "--batch-check=%(objecttype) %(objectsize) %(objectsize:disk)",
                "--batch-all-objects",
                "--unordered",
            ],
            encoding="utf-8",
            env=tool_environment(),
        ).splitlines()
        for line in all_object_types:
            typ, size, size_compressed = line.strip().split()
            size = int(size)
            size_compressed = int(size_compressed)
            if typ == "tree":
                num_trees += 1
                total_tree_size += size
                total_compressed_tree_size += size_compressed
            elif typ == "blob":
                num_blobs += 1
            uncompressed_repo_size += size

        compressed_repo_size = int(
            subprocess.check_output(("du", "-sc", str(path)))
            .splitlines()[-1]
            .split()[0]
        )
        # un-KiB
        compressed_repo_size *= 1024

        w.writerow(
            (
                name,
                is_hashed,
                branching_factor,
                num_levels,
                num_blobs,
                num_trees,
                # all in GB
                total_tree_size / 1_000_000_000.0,
                uncompressed_repo_size / 1_000_000_000.0,
                compressed_repo_size / 1_000_000_000.0,
                total_compressed_tree_size / 1_000_000_000.0,
            )
        )
