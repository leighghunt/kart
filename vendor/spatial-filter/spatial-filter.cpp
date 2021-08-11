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
    #include <list-objects-filter-profile.h>
    #include <hash.h>
    #include <trace.h>
}


#define EXPORT __attribute__((visibility("default")))

using std::string;
using std::vector;

static struct trace_key trace_filter = TRACE_KEY_INIT(FILTER);

static const string index_filename = "s2_index.db";

static const int s2_max_cells_query = 25;
static const int s2_max_level = 15;


enum match_result {
    mr_match,
    mr_not_matched,
    mr_error,
};

static enum match_result filter_check(sqlite3* db, sqlite3_stmt *stmt,
                                        const struct repository* r,
                                        const struct object_id *obj_id,
                                        const string &path) {
    if (path.find("/.sno-dataset/feature/") != string::npos) {
        int sqlErr = sqlite3_bind_blob(stmt, 1, obj_id->hash, r->hash_algo->rawsz, SQLITE_TRANSIENT);
        if (sqlErr) {
            std::cerr << "\nspatial-filter: Error: preparing lookup (" << sqlErr << " @0): " << sqlite3_errmsg(db) << "\n";
            return mr_error;
        }

        sqlErr = sqlite3_step(stmt);
        if (sqlErr != SQLITE_ROW) {
            std::cerr << "\nspatial-filter: Error: querying (" << sqlErr << "): " << sqlite3_errmsg(db) << "\n";
            sqlite3_reset(stmt);
            return mr_error;
        }

        const int isFound = sqlite3_column_int(stmt, 0);
        sqlite3_reset(stmt);

        // trace_printf_key(&trace_filter, " L-SQL: %s\n", sqlite3_expanded_sql(stmt));
        // trace_printf_key(&trace_filter, " L: %s [%zu] -> %d\n", oid_to_hex(obj_id), r->hash_algo->rawsz, isFound);

        if (isFound) {
            return mr_match;
        } else {
            return mr_not_matched;
        }
    }

    return mr_match;
}

//
// library interface
//

struct filter_context {
    int count = 0;
    int matchCount = 0;
    struct timespec t_start;
    sqlite3 *db = nullptr;
    sqlite3_stmt* lookup_stmt = nullptr;
};

EXPORT int git_filter_profile_init(const struct repository *r,
                                   const char *filter_arg,
                                   void **context) {
    struct filter_context *ctx = new filter_context();

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
        std::cerr << "spatial-filter: Error: invalid bounds, expected '<lat_s>,<lng_w>,<lat_n>,<lng_e>'\n";
        return 2;
    }
    S2LatLng sw = S2LatLng::FromDegrees(rect[0], rect[1]);
    S2LatLng ne = S2LatLng::FromDegrees(rect[2], rect[3]);

    if (!sw.is_valid() || !ne.is_valid()) {
        std::cerr << "spatial-filter: Error: invalid LatLng values, expected '<lat_s>,<lng_w>,<lat_n>,<lng_e>'\n";
        return 2;
    }
    sw = sw.Normalized();
    ne = ne.Normalized();
    trace_printf_key(&trace_filter, "SW=%s NE=%s\n",
        sw.ToStringInDegrees().c_str(), ne.ToStringInDegrees().c_str());

    S2LatLngRect s2_rect = S2LatLngRect::FromPointPair(sw, ne);

    std::ostringstream ss_db(r->gitdir, std::ios_base::ate);
    ss_db << "/" << index_filename;

    trace_printf_key(&trace_filter, "DB: %s\n", ss_db.str().c_str());
    if (sqlite3_open_v2(ss_db.str().c_str(), &ctx->db, SQLITE_OPEN_READONLY, NULL)) {
        std::cerr << "spatial-filter: Warning: not available for this repository - no objects will be omitted.\n";
        sqlite3_close(ctx->db);
        ctx->db = nullptr;
        return 0;
    }

    // prepare the query terms
    S2RegionTermIndexer::Options indexerOptions;
    indexerOptions.set_max_cells(s2_max_cells_query);
    indexerOptions.set_max_level(s2_max_level);
    S2RegionTermIndexer indexer(indexerOptions);

    const std::vector<string> queryTerms = indexer.GetQueryTerms(s2_rect, "");

    int sqlErr;

    // create and populate a temporary in-memory table to with the query terms
    sqlErr = sqlite3_exec(ctx->db,
                          "PRAGMA temp_store=MEMORY;"
                          "CREATE TEMP TABLE _query_cells (cell_token TEXT PRIMARY KEY);",
                          NULL, NULL, NULL);
    if (sqlErr) {
        std::cerr << "spatial-filter: Error: preparing query-cells (1/" << sqlErr << "): " << sqlite3_errmsg(ctx->db) << "\n";
        return 1;
    }

    sqlite3_stmt *stmt;
    sqlErr = sqlite3_prepare_v2(ctx->db,
                                "INSERT INTO _query_cells VALUES (?);",
                                -1,
                                &stmt,
                                NULL);
    if (sqlErr) {
        std::cerr << "spatial-filter: Error: preparing query-cells (2/" << sqlErr << "): " << sqlite3_errmsg(ctx->db) << "\n";
        return 1;
    }
    for (auto t: queryTerms) {
        trace_printf_key(&trace_filter, " TERM: %s\n", t.c_str());

        size_t offset = (t[0] == '$') ? 1 : 0;
        sqlErr = sqlite3_bind_text(stmt, 1, (t.c_str() + offset), -1, SQLITE_TRANSIENT);
        if (sqlErr) {
            std::cerr << "\nspatial-filter: Error: preparing query-cells (3/" << sqlErr << "): " << sqlite3_errmsg(ctx->db) << "\n";
            sqlite3_finalize(stmt);
            return 1;
        }

        sqlErr = sqlite3_step(stmt);
        if (sqlErr != SQLITE_DONE) {
            std::cerr << "\nspatial-filter: Error: populating query-cells (" << sqlErr << "): " << sqlite3_errmsg(ctx->db) << "\n";
            sqlite3_finalize(stmt);
            return 1;
        }
        sqlite3_reset(stmt);
    }
    sqlite3_finalize(stmt);

    // prepare the lookup db query
    const string querySql("SELECT EXISTS("
                          "SELECT 1 "
                          "FROM blobs "
                          "INNER JOIN blob_cells ON (blobs.rowid=blob_cells.blob_rowid) "
                          "INNER JOIN _query_cells ON (blob_cells.cell_token=_query_cells.cell_token) "
                          "WHERE blobs.blob_id=?);");
    sqlErr = sqlite3_prepare_v3(ctx->db,
                                querySql.c_str(),
                                static_cast<int>(querySql.size()+1),
                                SQLITE_PREPARE_PERSISTENT,
                                &ctx->lookup_stmt,
                                NULL);
    if (sqlErr) {
        std::cerr << "spatial-filter: Error: preparing lookup (" << sqlErr << ")" << sqlite3_errmsg(ctx->db) << "\n";
        return 1;
    }

    trace_printf_key(&trace_filter, "Query SQL: %s\n", sqlite3_expanded_sql(ctx->lookup_stmt));

    (*context) = ctx;
    return 0;
}

EXPORT enum list_objects_filter_result git_filter_profile_object(const struct repository *r,
                                                                const enum list_objects_filter_situation filter_situation,
                                                                struct object *obj,
                                                                const char *pathname,
                                                                const char *filename,
                                                                enum list_objects_filter_omit *omit,
                                                                void **context) {
    struct filter_context *ctx = static_cast<struct filter_context*>(*context);

    if (ctx->count == 0) {
        clock_gettime(CLOCK_MONOTONIC, &ctx->t_start);
    }
    if (++ctx->count % 20000 == 0) {
        std::cerr << "spatial-filter: " << ctx->count << "\r";
    }

    switch (filter_situation) {
        default:
            std::cerr << "spatial-filter: unknown filter_situation: " << filter_situation << "\n";
            abort();

        case LOFS_BEGIN_TREE:
            assert(obj->type == OBJ_TREE);
            /* always include all tree objects */
            return static_cast<list_objects_filter_result>(LOFR_MARK_SEEN | LOFR_DO_SHOW);

        case LOFS_END_TREE:
            assert(obj->type == OBJ_TREE);
            return LOFR_ZERO;

        case LOFS_BLOB:
            assert(obj->type == OBJ_BLOB);

            if (ctx->db == nullptr) {
                // we don't have a valid spatial index for this repository.
                // don't omit anything
                return static_cast<list_objects_filter_result>(LOFR_MARK_SEEN | LOFR_DO_SHOW);
            }

            switch(filter_check(ctx->db, ctx->lookup_stmt, r, &obj->oid, pathname)) {
                case mr_error:
                    abort();

                case mr_not_matched:
                    *omit = LOFO_OMIT;
                    return LOFR_MARK_SEEN;

                case mr_match:
                    ++ctx->matchCount;
                    return static_cast<list_objects_filter_result>(LOFR_MARK_SEEN | LOFR_DO_SHOW);
            }
    }
}

EXPORT void git_filter_profile_free(const struct repository* r, void **context) {
    struct filter_context *ctx = static_cast<struct filter_context*>(*context);

    struct timespec t_end;

    clock_gettime(CLOCK_MONOTONIC, &t_end);
    double elapsed = (t_end.tv_sec - ctx->t_start.tv_sec) + (t_end.tv_nsec - ctx->t_start.tv_nsec)/1E9;

    std::cerr << "spatial-filter: " << ctx->count << "\n";
    trace_printf_key(
        &trace_filter,
        "count=%d matched=%d elapsed=%fs rate=%f/s average=%fus\n",
        ctx->count, ctx->matchCount, elapsed, ctx->count/elapsed, elapsed/ctx->count*1E6
    );

    if (ctx->lookup_stmt != nullptr) {
        sqlite3_finalize(ctx->lookup_stmt);
    }
    if (ctx->db != nullptr) {
        sqlite3_close_v2(ctx->db);
    }
    delete ctx;
}
