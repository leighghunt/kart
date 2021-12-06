#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <time.h>

#include <s2/s2cell_id.h>
#include <s2/s2cell_union.h>
#include <s2/s2latlng_rect.h>
#include <s2/s2region_term_indexer.h>

#include <sqlite3.h>

extern "C" {
    #include <list-objects-filter-extensions.h>
    #include "adapter_functions.h"
}

using std::string;
using std::vector;

namespace {

static const string INDEX_FILENAME = "feature_envelopes.db";

static const int OBJ_COMMIT = 1;
static const int OBJ_TREE = 2;
static const int OBJ_BLOB = 3;
static const int OBJ_TAG = 4;

enum match_result {
    MR_MATCH,
    MR_NOT_MATCHED,
    MR_ERROR,
};

struct filter_context {
    int count = 0;
    int match_count = 0;
    uint64_t started_at = 0;
    sqlite3 *db = nullptr;
    sqlite3_stmt* lookup_stmt = nullptr;
    double w = 0, s = 0, e = 0, n = 0;
};

bool range_overlaps(double a1, double a2, double b1, double b2) {
    if (a1 > a2 || b1 > b2) {
        std::cerr << "Ranges don't make sense: " << a1 << " " << a2 << " " << b1 << " " << b2 << "\n";
        abort();
    }
    if (b1 < a1) {
        // `b` starts to the left of `a`, so they intersect if `b` finishes to the right of where `a` starts.
        return b2 > a1;
    }
    if (a1 < b1) {
        // `a` starts to the left of `b`, so they intersect if `a` finishes to the right of where `b` starts.
        return a2 > b1;
    }
    // They both have the same left edge, so they must intersect unless one of them is zero-width.
    return b2 != b1 && a2 != a1;
}

// Core function - decides whether a blob matches or not.

enum match_result sf_filter_blob(
    struct filter_context *ctx,
    const struct repository* repo,
    const struct object_id *oid,
    const string &path)
{
    // We are only spatial-filtering features - all non-feature data matches automatically.
    if (path.find("/.sno-dataset/feature/") == string::npos
        && path.find("/.table-dataset/feature/") == string::npos) {
        return MR_MATCH;
    }

    sqlite3 *db = ctx->db;
    sqlite3_stmt *stmt = ctx->lookup_stmt;

    int sql_err = sqlite3_bind_blob(stmt, 1, sf_oid2hash(oid), sf_repo2hashsz(repo), SQLITE_TRANSIENT);
    if (sql_err) {
        std::cerr << "\nspatial-filter: Error: preparing lookup (" << sql_err << " @0): " << sqlite3_errmsg(db) << "\n";
        return MR_ERROR;
    }

    sql_err = sqlite3_step(stmt);
    if (sql_err == SQLITE_DONE) {
        sqlite3_reset(stmt);
        return MR_MATCH;
    }
    if (sql_err != SQLITE_ROW) {
        std::cerr << "\nspatial-filter: Error: querying (" << sql_err << "): " << sqlite3_errmsg(db) << "\n";
        sqlite3_reset(stmt);
        return MR_ERROR;
    }

    const double w = sqlite3_column_double(stmt, 0);
    const double s = sqlite3_column_double(stmt, 1);
    const double e = sqlite3_column_double(stmt, 2);
    const double n = sqlite3_column_double(stmt, 3);

    bool overlaps = range_overlaps(w, e, ctx->w, ctx->e) && range_overlaps(s, n, ctx->s, ctx->n);

    sqlite3_reset(stmt);

    return overlaps ? MR_MATCH : MR_NOT_MATCHED;
}

//
// Filter extension interface:
//

int sf_init(
    const struct repository *r,
    const char *filter_arg,
    void **context)
{
    std::vector<double> rect;
    std::stringstream ss_arg(filter_arg);
    double d;

    while (ss_arg >> d)
    {
        rect.push_back(d);
        if (ss_arg.peek() == ',')
            ss_arg.ignore();
    }
    if (rect.size() != 4) {
        std::cerr << "spatial-filter: Error: invalid bounds, expected '<lng_w>,<lat_s>,<lng_e>,<lat_n>'\n";
        return 2;
    }

    std::ostringstream ss_db(sf_repo2gitdir(r), std::ios_base::ate);
    ss_db << "/" << INDEX_FILENAME;

    sf_trace_printf("DB: %s\n", ss_db.str().c_str());

    struct filter_context *ctx = new filter_context();
    (*context) = ctx;
    ctx->w = rect[0];
    ctx->s = rect[1];
    ctx->e = rect[2];
    ctx->n = rect[3];

    if (sqlite3_open_v2(ss_db.str().c_str(), &ctx->db, SQLITE_OPEN_READONLY, NULL)) {
        std::cerr << "spatial-filter: Warning: not available for this repository - no objects will be omitted.\n";
        sqlite3_close(ctx->db);
        ctx->db = nullptr;
        return 0;
    }

    int sql_err;
    sqlite3_stmt *stmt;

    // prepare the lookup db query
    const string query_sql("SELECT w, s, e, n FROM blobs WHERE blobs.blob_id=?;");
    sql_err = sqlite3_prepare_v3(ctx->db,
                                 query_sql.c_str(),
                                 static_cast<int>(query_sql.size()+1),
                                 SQLITE_PREPARE_PERSISTENT,
                                 &ctx->lookup_stmt,
                                 NULL);
    if (sql_err) {
        std::cerr << "spatial-filter: Error: preparing lookup (" << sql_err << ") " << sqlite3_errmsg(ctx->db) << "\n";
        return 1;
    }

    sf_trace_printf("Query SQL: %s\n", sqlite3_expanded_sql(ctx->lookup_stmt));

    (*context) = ctx;
    return 0;
}

enum list_objects_filter_result sf_filter_object(
    const struct repository *repo,
    const enum list_objects_filter_situation filter_situation,
    struct object *obj,
    const char *pathname,
    const char *filename,
    enum list_objects_filter_omit *omit,
    void *context)
{
    struct filter_context *ctx = static_cast<struct filter_context*>(context);

    static const list_objects_filter_result LOFR_MARK_SEEN_AND_DO_SHOW =
        static_cast<list_objects_filter_result>(LOFR_MARK_SEEN | LOFR_DO_SHOW);

    if (ctx->count == 0) {
        ctx->started_at = getnanotime();
    }
    if (++ctx->count % 20000 == 0) {
        std::cerr << "spatial-filter: " << ctx->count << "\r";
    }

    switch (filter_situation) {
        default:
            std::cerr << "spatial-filter: unknown filter_situation: " << filter_situation << "\n";
            abort();

        case LOFS_COMMIT:
            assert(sf_obj2type(obj) == OBJ_COMMIT);
            return LOFR_MARK_SEEN_AND_DO_SHOW;

        case LOFS_TAG:
            assert(sf_obj2type(obj) == OBJ_TAG);
            return LOFR_MARK_SEEN_AND_DO_SHOW;

        case LOFS_BEGIN_TREE:
            assert(sf_obj2type(obj) == OBJ_TREE);
            // Always include all tree objects.
            return LOFR_MARK_SEEN_AND_DO_SHOW;

        case LOFS_END_TREE:
            assert(sf_obj2type(obj) == OBJ_TREE);
            return LOFR_ZERO;

        case LOFS_BLOB:
            assert(sf_obj2type(obj) == OBJ_BLOB);

            if (ctx->db == nullptr) {
                // We don't have a valid spatial index for this repository. Don't omit anything.
                return LOFR_MARK_SEEN_AND_DO_SHOW;
            }

            switch(sf_filter_blob(ctx, repo, sf_obj2oid(obj), pathname)) {
                case MR_ERROR:
                    abort();

                case MR_NOT_MATCHED:
                    *omit = LOFO_OMIT;
                    return LOFR_MARK_SEEN;

                case MR_MATCH:
                    ++ctx->match_count;
                    return LOFR_MARK_SEEN_AND_DO_SHOW;
            }
    }
}

void sf_free(const struct repository* r, void *context) {
    struct filter_context *ctx = static_cast<struct filter_context*>(context);

    double elapsed = (getnanotime() - ctx->started_at) / 1e9;
    std::cerr << "spatial-filter: " << ctx->count << "\n";
    sf_trace_printf(
        "count=%d matched=%d elapsed=%fs rate=%f/s average=%fus\n",
        ctx->count, ctx->match_count, elapsed, ctx->count/elapsed, elapsed/ctx->count*1e6
    );

    if (ctx->lookup_stmt != nullptr) {
        sqlite3_finalize(ctx->lookup_stmt);
    }
    if (ctx->db != nullptr) {
        sqlite3_close_v2(ctx->db);
    }
    delete ctx;
}

}  // namespace

extern "C" {
extern const struct filter_extension filter_extension_spatial = {
    "spatial",
    &sf_init,
    &sf_filter_object,
    &sf_free,
};
}