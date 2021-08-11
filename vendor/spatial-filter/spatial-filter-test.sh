#!/bin/bash
set -eu

echo "Found git @ $(realpath "$(which git)") and kart @ $(realpath "$(which kart)")"

USAGE="Usage: $0 cpp|noop|rand-git|rand-git-cpp|blob-none|all /path/to/source/repo.git /path/to/dest/repo [test|trace|kart]\nEnsure your custom git is at the front of \$PATH"

if [ $# -lt 3 ]; then
	echo -e "$USAGE"
	exit 2
fi

SRC=$2
if [ ! -d "$SRC" ]; then
	echo "$SRC isn't a directory"
	exit 2
fi

DEST=$3
if [ -e "$DEST" ] && [ ! -d "$DEST" ]; then
	echo "$DEST isn't a directory"
	exit 2
fi

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
FILTER_ROOT="$SCRIPT_DIR"
FILTER_PY="$FILTER_ROOT/kart-spatial-filter-profile.py"
CLONE_CMD="git clone --bare"
KART_CLONE_CMD="kart clone"


# enable filtering
git -C "$SRC" config --local "uploadpack.allowFilter" "true"

FILTER_PROFILE=$1
if [ "$FILTER_PROFILE" == "cpp" ]; then
	git -C "$SRC" config --local "uploadpackfilter.profile:spatial.plugin" "$FILTER_ROOT/spatial-filter.so"
	FILTER_ARG="--filter=profile:spatial=${FILTER_VALUE--41.0289494,174.8987033,-41.0289494,174.8987033}"
	FILTER_PROFILE=spatial
elif [ "$FILTER_PROFILE" == "noop" ]; then
	git -C "$SRC" config --local "uploadpackfilter.profile:noop.plugin" "$FILTER_ROOT/filter-noop.dylib"
	FILTER_ARG="--filter=profile:noop=${FILTER_VALUE-1}"
elif [ "$FILTER_PROFILE" == "rand-git" ]; then
	git -C "$SRC" config --local "uploadpackfilter.profile:rand.plugin" "contrib/filter-profiles/rand/rand.so"
	FILTER_ARG="--filter=profile:rand=${FILTER_VALUE-1}"
elif [ "$FILTER_PROFILE" == "rand-git-cpp" ]; then
	git -C "$SRC" config --local "uploadpackfilter.profile:rand-cpp.plugin" "contrib/filter-profiles/rand-cpp/rand-cpp.so"
	FILTER_ARG="--filter=profile:rand-cpp=${FILTER_VALUE-1}"
elif [ "$FILTER_PROFILE" == "rand++" ]; then
	git -C "$SRC" config --local "uploadpackfilter.profile:randcpp.plugin" "$FILTER_ROOT/filter-rand++.so"
	FILTER_ARG="--filter=profile:randcpp=${FILTER_VALUE-1}"
elif [ "$FILTER_PROFILE" == "blob-none" ]; then
	FILTER_ARG="--filter=blob:none"
elif [ "$FILTER_PROFILE" == "all" ]; then
	FILTER_ARG=
else
	echo -e "$USAGE"
	exit 2
fi

: "${DEPTH:=1}"

IS_TEST=0
IS_TRACE=0
if [ "${4-}" == "test" ]; then
	IS_TEST=1
elif [ "${4-}" == "trace" ]; then
	IS_TRACE=1
elif [ "${4-}" == "kart" ]; then
	CLONE_CMD=$KART_CLONE_CMD
elif [ -n "${4-}" ]; then
	echo -e "$USAGE"
	exit 2
fi

if [ "$FILTER_PROFILE" == "spatial" ]; then
	if [ -f "$SRC/s2_index.db" ]; then
		echo "🔸 found existing s2_index.db for $2 ($(du -sh "$SRC/s2_index.db"))"
	else
		echo "🔸 building s2_index.db for $2 ..."
		(cd "$SRC"; time kart --verbose spatial-tree index | "$FILTER_PY" write)
	fi
fi

if [ $IS_TRACE -eq 1 ]; then
	rm -rf rev-list.trace
	xcrun xctrace record \
		--output rev-list.trace \
		--template 'Time Profiler' \
		--env "PATH=$PATH" \
		--launch -- \
		"$(which git)" \
		-C "$SRC" \
		rev-list \
		refs/heads/master \
		--objects \
		--max-count "$DEPTH" \
		$FILTER_ARG \
		--filter-print-omitted
	exec open rev-list.trace
	exit 0
elif [ $IS_TEST -eq 1 ]; then
	echo "🔸 filtering $2 with 'git rev-list'"
	time \
	git -C "$SRC" rev-list \
		refs/heads/master \
		--objects \
		--max-count "$DEPTH" \
		$FILTER_ARG \
		--filter-print-omitted \
	| awk 'BEGIN {OC=0; MC=0}
		{ if ($1 ~ /^~/) {OC++;} else {MC++;} }
		END { print "Total:" NR "\nFiltered-out:" OC "\nFiltered-in:" MC }'
	exit 0
fi

# do a clone

if [ -d "$DEST" ]; then
	echo "🔸 removing existing $DEST"
	rm -rf "$DEST"
fi

echo "🔸 source object count"
git -C "$SRC" count-objects -vH

echo "🔸 cloning $2 ($CLONE_CMD)..."
time $CLONE_CMD \
	--depth "$DEPTH" \
	$FILTER_ARG \
	"file://$SRC" "$DEST"

echo "🔸 dest object count"
git -C "$DEST" count-objects -vH

DEST_MISSING_COUNT=$(git -C "$DEST" rev-list --all --quiet --objects --missing=print | wc -l)
echo "🔸 filtered-out (missing) objects: $DEST_MISSING_COUNT"

if [ "$CLONE_CMD" == "$KART_CLONE_CMD" ]; then
	echo "🔸 Working Copy GeoPackage:"
	SQLEXEC="sqlite3 -batch -list -noheader $DEST/*.gpkg"
	for TABLE in $($SQLEXEC "SELECT table_name FROM gpkg_contents;"); do
		echo -e "---\n$TABLE"
		echo "rows: $($SQLEXEC "SELECT COUNT(*) FROM $TABLE;")"
		GEOM_COL=$($SQLEXEC "SELECT column_name FROM gpkg_geometry_columns WHERE table_name='$TABLE';")
		if [ -n "$GEOM_COL" ]; then
			$SQLEXEC <<-EOF
			SELECT load_extension('${SCRIPT_DIR}/../../venv/lib/mod_spatialite');
			SELECT EnableGPKGMode();
			SELECT ST_AsText(Extent($GEOM_COL)) FROM $TABLE;
			EOF
		fi
	done
fi

echo "🔸 bye 😀"
