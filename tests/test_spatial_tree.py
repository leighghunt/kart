import pytest

from kart import is_windows, is_linux
from kart.sqlalchemy.sqlite import sqlite_engine
from sqlalchemy.orm import sessionmaker

H = pytest.helpers.helpers()

SKIP_REASON = "s2_py is not yet included in the kart windows build"


@pytest.mark.skipif(is_windows, reason=SKIP_REASON)
def test_isolated():
    w, s, e, n = (174.7864396833, -41.2521621333, 174.7938725833, -41.2476486833)
    import s2_py as s2

    s2_indexer = s2.S2RegionTermIndexer()
    s2_indexer.set_min_level(4)
    s2_indexer.set_max_level(16)
    s2_indexer.set_level_mod(1)
    s2_indexer.set_max_cells(8)

    s2_ll = []
    s2_ll.append(s2.S2LatLng.FromDegrees(s, w).Normalized())
    s2_ll.append(s2.S2LatLng.FromDegrees(n, e).Normalized())
    s2_llrect = s2.S2LatLngRect.FromPointPair(*s2_ll)
    query_terms = s2_indexer.GetIndexTerms(s2_llrect, "")

    def key(term):
        out = ""
        if term[0] == "$":
            out = "$"
            term = term[1:]
        b = bin(int(term, 16))
        return str(len(b)) + b + out

    assert sorted(query_terms, key=key) == [
        '6d3',
        '6d39',
        '6d3c',
        '6d38b',
        '6d38c',
        '6d38ac',
        '6d38af',
        '6d38ae1',
        '6d38ae4',
        '6d38ae09',
        '6d38ae0b',
        '6d38ae0c',
        '6d38ae0d',
        '$6d38ae0d',
        '6d38ae0f',
        '6d38ae093',
        '6d38ae094',
        '6d38ae095',
        '6d38ae0b4',
        '6d38ae0b5',
        '6d38ae0b7',
        '6d38ae0bc',
        '$6d38ae0bc',
        '6d38ae0e4',
        '$6d38ae0e4',
        '6d38ae0ec',
        '$6d38ae0ec',
    ]


@pytest.mark.skipif(is_windows, reason=SKIP_REASON)
def test_index_points_all(data_archive, cli_runner):
    # Indexing --all should give the same results every time.
    # For points, every point should have only one long S2 cell token.
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr

        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 2148
        assert stats.avg_s2_tokens_per_feature == 13.0
        assert stats.avg_s2_token_length == pytest.approx(6.231, abs=0.001)
        assert stats.distinct_s2_tokens == 10980


@pytest.mark.skipif(is_windows, reason=SKIP_REASON)
def test_index_points_commit_by_commit(data_archive, cli_runner):
    # Indexing one commit at a time should get the same results as indexing --all.
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD1_SHA])
        assert r.exit_code == 0, r.stderr
        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 2143

        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD_SHA])
        assert r.exit_code == 0, r.stderr

        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 2148
        assert stats.avg_s2_tokens_per_feature == 13.0
        assert stats.avg_s2_token_length == pytest.approx(6.231, abs=0.001)
        assert stats.distinct_s2_tokens == 10980


@pytest.mark.skipif(is_windows, reason=SKIP_REASON)
def test_index_points_idempotent(data_archive, cli_runner):
    # Indexing the commits one at a time and then indexing all commits again will also give the same result.
    # (We force everything to be indexed twice by deleting the record of whats been indexed).
    with data_archive("points.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD1_SHA])
        assert r.exit_code == 0, r.stderr
        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 2143

        r = cli_runner.invoke(["spatial-tree", "index", H.POINTS.HEAD_SHA])
        assert r.exit_code == 0, r.stderr
        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 2148

        # Trying to reindex shouldn't do anything since we remember where we are up to.
        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr
        assert "Nothing to do" in r.stdout
        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 2148

        # Force reindex by deleting record of what's been indexed.
        # Even so, this should just rewrite the same index over the top of the old one.
        db_path = repo_path / ".kart" / "s2_index.db"
        engine = sqlite_engine(db_path)
        with sessionmaker(bind=engine)() as sess:
            sess.execute("DELETE FROM commits;")

        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr
        assert "Nothing to do" not in r.stdout
        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 2148
        assert stats.avg_s2_tokens_per_feature == 13.0
        assert stats.avg_s2_token_length == pytest.approx(6.231, abs=0.001)
        assert stats.distinct_s2_tokens == 10980


@pytest.mark.skipif(is_windows, reason=SKIP_REASON)
def test_index_polygons_all(data_archive, cli_runner):
    # FIXME: These results shouldn't be different on macos and linux.
    # Dig into why they are different.
    with data_archive("polygons.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr

        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 228
        assert stats.avg_s2_tokens_per_feature == pytest.approx(
            30.101 if is_linux else 30.075, abs=0.001
        )
        assert stats.avg_s2_token_length == pytest.approx(
            7.290 if is_linux else 7.292, abs=0.001
        )
        assert stats.distinct_s2_tokens == 3812 if is_linux else 3802


@pytest.mark.skipif(is_windows, reason=SKIP_REASON)
def test_index_table_all(data_archive, cli_runner):
    with data_archive("table.tgz") as repo_path:
        r = cli_runner.invoke(["spatial-tree", "index"])
        assert r.exit_code == 0, r.stderr

        stats = _get_spatial_tree_stats(repo_path)
        assert stats.features == 0
        assert stats.s2_tokens == 0


def _get_spatial_tree_stats(repo_path):
    class Stats:
        pass

    stats = Stats()

    db_path = repo_path / ".kart" / "s2_index.db"
    engine = sqlite_engine(db_path)
    with sessionmaker(bind=engine)() as sess:
        orphans = sess.execute(
            """
            SELECT blob_rowid FROM blob_tokens
            EXCEPT SELECT rowid FROM blobs;
            """
        )
        assert orphans.first() is None

        stats.features = sess.scalar("SELECT COUNT(*) FROM blobs;")
        stats.s2_tokens = sess.scalar("SELECT COUNT(*) FROM blob_tokens;")

        if stats.features:
            stats.avg_s2_tokens_per_feature = stats.s2_tokens / stats.features

        if stats.s2_tokens:
            stats.avg_s2_token_length = sess.scalar(
                "SELECT AVG(LENGTH(s2_token)) FROM blob_tokens;"
            )
            stats.distinct_s2_tokens = sess.scalar(
                "SELECT COUNT (DISTINCT s2_token) FROM blob_tokens;"
            )

    return stats
