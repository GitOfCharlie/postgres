/*-------------------------------------------------------------------------
 *
 * indexrec.c
 *	  do index recommendation during exec query
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/index/indexrec.c
 *
 * INTERFACE ROUTINES
 *		//		- //
 *
 * NOTES
 *		//
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/indexrec.h"
#include "access/sqlstruct.h"
/* for define index */
#include "access/xact.h"
#include "catalog/index.h"
#include "catalog/objectaddress.h"
#include "catalog/namespace.h"
#include "catalog/pg_class_d.h"
#include "catalog/pg_inherits.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/event_trigger.h"
#include "parser/parse_utilcmd.h"
#include "utils/lsyscache.h"


/* functions of recommendation */
static void index_recommendation_with_ref(RawStmt *rawStmt, RawStmt *refStmt);
static bool is_struct_similar_query(RawStmt *stmt1, RawStmt *stmt2);
static void generate_index(IndexStmt *indexStmt);


/* Check whether the stmt needs to recommend */
bool query_not_involve_system_relation(RawStmt *rawStmt)
{
    return !stmt_contains_system_relation(rawStmt);
}

/* index recommendation functions */
static void
generate_index(IndexStmt *stmt)
{
    bool		isTopLevel = true;
	ObjectAddress address;
	ObjectAddress secondaryObject = InvalidObjectAddress;

    Oid			relid;
    LOCKMODE	lockmode;
    bool		is_alter_table;

    if (stmt->concurrent)
        PreventInTransactionBlock(isTopLevel,
                                    "CREATE INDEX CONCURRENTLY");

    /*
        * Look up the relation OID just once, right here at the
        * beginning, so that we don't end up repeating the name
        * lookup later and latching onto a different relation
        * partway through.  To avoid lock upgrade hazards, it's
        * important that we take the strongest lock that will
        * eventually be needed here, so the lockmode calculation
        * needs to match what DefineIndex() does.
        */
    lockmode = stmt->concurrent ? ShareUpdateExclusiveLock
        : ShareLock;
    relid =
        RangeVarGetRelidExtended(stmt->relation, lockmode,
                                    0,
                                    RangeVarCallbackOwnsRelation,
                                    NULL);

    /*
        * CREATE INDEX on partitioned tables (but not regular
        * inherited tables) recurses to partitions, so we must
        * acquire locks early to avoid deadlocks.
        *
        * We also take the opportunity to verify that all
        * partitions are something we can put an index on, to
        * avoid building some indexes only to fail later.
        */
    if (stmt->relation->inh &&
        get_rel_relkind(relid) == RELKIND_PARTITIONED_TABLE)
    {
        ListCell   *lc;
        List	   *inheritors = NIL;

        inheritors = find_all_inheritors(relid, lockmode, NULL);
        foreach(lc, inheritors)
        {
            char		relkind = get_rel_relkind(lfirst_oid(lc));

            if (relkind != RELKIND_RELATION &&
                relkind != RELKIND_MATVIEW &&
                relkind != RELKIND_PARTITIONED_TABLE &&
                relkind != RELKIND_FOREIGN_TABLE)
                elog(ERROR, "unexpected relkind \"%c\" on partition \"%s\"",
                        relkind, stmt->relation->relname);

            if (relkind == RELKIND_FOREIGN_TABLE &&
                (stmt->unique || stmt->primary))
                ereport(ERROR,
                        (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                            errmsg("cannot create unique index on partitioned table \"%s\"",
                                stmt->relation->relname),
                            errdetail("Table \"%s\" contains partitions that are foreign tables.",
                                    stmt->relation->relname)));
        }
        list_free(inheritors);
    }

    /*
        * If the IndexStmt is already transformed, it must have
        * come from generateClonedIndexStmt, which in current
        * usage means it came from expandTableLikeClause rather
        * than from original parse analysis.  And that means we
        * must treat it like ALTER TABLE ADD INDEX, not CREATE.
        * (This is a bit grotty, but currently it doesn't seem
        * worth adding a separate bool field for the purpose.)
        */
    is_alter_table = stmt->transformed;

    /* Run parse analysis ... */
    stmt = transformIndexStmt(relid, stmt, NULL); //querystring to be NULL

    /* ... and do it */
    EventTriggerAlterTableStart((Node*)stmt);
    address =
        DefineIndex(relid,	/* OID of heap relation */
                    stmt,
                    InvalidOid, /* no predefined OID */
                    InvalidOid, /* no parent index */
                    InvalidOid, /* no parent constraint */
                    is_alter_table,
                    true,	/* check_rights */
                    true,	/* check_not_in_use */
                    false,	/* skip_build */
                    false); /* quiet */

    /*
        * Add the CREATE INDEX node itself to stash right away;
        * if there were any commands stashed in the ALTER TABLE
        * code, we need them to appear after this one.
        */
    EventTriggerCollectSimpleCommand(address, secondaryObject,
                                        (Node*)stmt);
    EventTriggerAlterTableEnd();
}

static void
index_recommendation_with_ref(RawStmt *rawStmt, RawStmt *refStmt){
    // List        *existIndexs; // List of IndexStmt
    // ListCell    *l;
    // // existIndexs = getIndexsInCurrentDb(Oid dboid); TODO

    // foreach(l, existIndexs)
    // {
    //     ;
    // }

    List        *rawColList = get_columns_from_stmt(rawStmt);
    List        *refColList = get_columns_from_stmt(refStmt);
    //TODO


}

static bool
is_struct_similar_query(RawStmt *stmt1, RawStmt *stmt2){
    StmtFeatureVec    stmtFeature1, stmtFeature2;

    construct_stmt_feature_vector(stmt1, &stmtFeature1);
    construct_stmt_feature_vector(stmt2, &stmtFeature2);
    
    return stmt_feature_equal(&stmtFeature1, &stmtFeature2);
}

/* outer interface for index recommendation */
void 
index_recommend(RawStmt *rawStmt){
    if (!IsA(rawStmt->stmt, SelectStmt) 
        // && !IsA(rawStmt->stmt, InsertStmt)
        // && !IsA(rawStmt->stmt, UpdateStmt)
        // && !IsA(rawStmt->stmt, DeleteStmt)
        )
        return;


    // RawStmt     *refStmt;
    // List        *queryRefList;
    // // queryRefList = getHistoryQueriesInDb(Oid dboid); TODO, sth like this
    // ListCell    *lc;

    // foreach(lc, queryRefList)
    // {
    //     refStmt = lfirst_node(RawStmt, lc);
    //     if (is_struct_similar_query(rawStmt, refStmt))
    //         index_recommendation_with_ref(rawStmt, refStmt);

    // }
    
}

void 
index_recommend_simple(RawStmt *rawStmt)
{
    if (!IsA(rawStmt->stmt, SelectStmt) 
        // && !IsA(rawStmt->stmt, InsertStmt)
        // && !IsA(rawStmt->stmt, UpdateStmt)
        // && !IsA(rawStmt->stmt, DeleteStmt)
        )
        return;

    List        *allColList = get_columns_from_stmt(rawStmt); /* All columns (StmtColumnInfo) */
    List        *colListByRelation = group_columns_by_relation(allColList); /* Group the columns by relation */
    
    List        *colList = NIL;
    ListCell    *lc, *lc2;
    RangeVar    *relation;
    IndexStmt   *idxStmt;

    if(colListByRelation == NIL)
        return;
    
    /* For each relation, generate the IndexStmt */
    foreach(lc, colListByRelation)
    {
        colList = (List*)lfirst(lc);
        relation = ((StmtColumnInfo*) linitial(colList))->rel;

        idxStmt = makeNode(IndexStmt);
        idxStmt->relation = relation;
        idxStmt->accessMethod = DEFAULT_INDEX_TYPE;
        foreach(lc2, colList)
        {
            StmtColumnInfo *colInfo = (StmtColumnInfo*) lfirst(lc2);
            IndexElem       *idxElem = makeNode(IndexElem);
            idxElem->name = colInfo->colName;
            idxStmt->indexParams = lappend(idxStmt->indexParams, idxElem);
        }

        generate_index(idxStmt);
    }

}