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
#include "access/indexinfo.h"
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
#include "port.h"
#include "tcop/tcopprot.h"
#include "utils/lsyscache.h"
#include <string.h>

List *history_queries;


/* functions of recommendation */
static void generate_index(IndexStmt *indexStmt);

static void construct_bipartite_conn(List *rawColList, List *refColList);
static List *append_permutation_in_bipartite(List *refColList, List *tempColList, List *resultList, int currDepth, int maxDepth);

static bool is_prefix_index(List *longIdx, List *shortIdx);
static bool is_prefix_index_existing(List *existIdx, List *candidateIdx);
static int index_col_list_length_compare(const ListCell *a, const ListCell *b);
static List *drop_duplicate_index(List *indexList, List *relationList);

static char *create_index_name_irec(List *colList);
static void index_recommendation_with_ref(RawStmt *rawStmt, RawStmt *refStmt);
static bool is_struct_similar_query(RawStmt *stmt1, RawStmt *stmt2);



/* generate an index in the backend */
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


/* index recommendation functions */

/* 
 * Construct bipartite edges between columns of stmt and ref stmt.
 * If col1, col2 both appear from one same predicate, then add an edge (col1, col2)
 */
static void
construct_bipartite_conn(List *rawColList, List *refColList)
{
    ListCell        *lc, *lc2;
    StmtColumnInfo  *rawCol, *refCol;
    foreach(lc, rawColList)
    {
        rawCol = lfirst(lc);
        foreach(lc2, refColList)
        {
            refCol = lfirst(lc2);
            if(has_common_from_predicate(rawCol, refCol))
            {
                /* Reference each other */
                rawCol->connectCol = lappend(rawCol->connectCol, refCol);
                refCol->connectCol = lappend(refCol->connectCol, rawCol);
            }
        }
    }
}

/*  
 * Add permutation of columns by referred columns
 */
static List *
append_permutation_in_bipartite(List *refColList, List *tempColList, List *resultList, int currDepth, int maxDepth)
{
    StmtColumnInfo  *refCol, *recCol, *prevCol;
    ListCell        *lc, *lc2;
    List            *copyList;

    if(currDepth == maxDepth)
    {
        resultList = lappend(resultList, tempColList);
        return resultList;
    }
    
    refCol = (StmtColumnInfo*)list_nth(refColList, currDepth);
    foreach(lc, refCol->connectCol)
    {
        recCol = (StmtColumnInfo*)lfirst(lc);
        /* If the column is in the same relation with previous column */
        if(tempColList == NIL || recCol->relid == ((StmtColumnInfo*)linitial(tempColList))->relid)
        {
            bool    isDuplicate = false;
            /* If this column has appreared before, skip it. */
            foreach(lc2, tempColList)
            {
                prevCol = (StmtColumnInfo*)lfirst(lc2);
                if(prevCol->attrNum == recCol->attrNum)
                {
                    isDuplicate = true;
                    break;
                }
            }
            copyList = list_copy(tempColList);
            if(!isDuplicate)
                copyList = lappend(copyList, recCol);
            
            resultList = append_permutation_in_bipartite(refColList, copyList, resultList, currDepth + 1, maxDepth);
        }
        else /* Else, append current tempList(its length may be < maxDepth) */
        {
            resultList = lappend(resultList, tempColList);
        }
    }

    return resultList;
}


/*
 * Check whether shortIdx is longIdx's prefix or the same.
 * Same index is a special prefix index.
 * Two params are lists of StmtColumnInfo*.
 */
static bool
is_prefix_index(List *longIdx, List *shortIdx)
{
    ListCell        *lc1, *lc2;
    StmtColumnInfo  *col1, *col2;

    if(list_length(longIdx) < list_length(shortIdx))
        return false;
    
    forboth(lc1, longIdx, lc2, shortIdx)
    {
        col1 = (StmtColumnInfo*)lfirst(lc1);
        col2 = (StmtColumnInfo*)lfirst(lc2);
        if(col1->relid != col2->relid || col1->attrNum != col2->attrNum)
            return false;
    }
    return true;
}

/*
 * Same as 'is_prefix_index', but param 'existIdx' is a list of ColAttrInfo*
 * because we compare a candidate index with an existing one.
 * We have already make sure that they target at the same relation. So there's no need to compare relids.
 */
static bool
is_prefix_index_existing(List *existIdx, List *candidateIdx)
{
    ListCell        *lc1, *lc2;
    ColAttrInfo     *col1;
    StmtColumnInfo  *col2;

    if(list_length(existIdx) < list_length(candidateIdx))
        return false;
    
    forboth(lc1, existIdx, lc2, candidateIdx)
    {
        col1 = (ColAttrInfo*)lfirst(lc1);
        col2 = (StmtColumnInfo*)lfirst(lc2);
        if(col1->attrNum != col2->attrNum)
            return false;
    }
    return true;
}

/*
 * Index's column list length comparator
 */
static int
index_col_list_length_compare(const ListCell *a, const ListCell *b)
{
    List        *idx1 = (List*)lfirst(a);
    List        *idx2 = (List*)lfirst(b);

    if(list_length(idx1) == list_length(idx2))
        return 0;
    return (list_length(idx1) < list_length(idx2)) ? +1 : -1;
}

/*
 * Postprocess the indexes list to be recommend. 
 * Drop duplicate indexes(same candidate idx or other candidate's prefix), existing indexes(same or prefix).
 * 
 * Param 'indexList' is a list of List(StmtColInfo*), containing indexes to be recommended.
 * Param 'relationList' is a list of relation(RangeVar*), containing relations involved in raw stmt,
 * which are used to check duplications with existing indexes.
 */
static List *
drop_duplicate_index(List *indexList, List *relationList)
{   
    List                *lidx, *sidx;
    List                *relIndexInfoList = NIL; /* A list of RelationIndexInfo*, index info of each relation in relationList */
    RelationIndexInfo   *rIndexInfo;
    IndexColsInfo       *icolsInfo;
    ListCell            *lc, *lc2, *lc3;

    if(indexList == NIL || list_length(indexList) <= 1)
        return indexList;
    
    /* Sort list by length desc */
    list_sort(indexList, index_col_list_length_compare);

    /* Drop duplicate ones (inner-list compare) */
    foreach(lc, indexList)
    {
        lidx = (List*)lfirst(lc);
        if(lnext(indexList, lc) != NULL)
        {
            /* loop from the next cell to drop duplicate ones */
            for_each_cell(lc2, indexList, lnext(indexList, lc))
            {
                sidx = (List*)lfirst(lc2);
                /* Compare the columns of 2 indexes to check prefix (lidx len >= sidx len) */
                if(is_prefix_index(lidx, sidx))
                {
                    /* Drop the shorter candidate index */
                    indexList = foreach_delete_current(indexList, lc2);
                }
            }
        }
    }

    /* Drop existing ones (outer-list compare, compared with existing indexes) */
    foreach(lc, relationList)
    {
        rIndexInfo = get_index_list_in_rel((RangeVar*)lfirst(lc));
        relIndexInfoList = lappend(relIndexInfoList, rIndexInfo);
    }

    foreach(lc, indexList)
    {
        /* Get an index (col list and relid) */
        lidx = (List*)lfirst(lc);
        Oid relid = ((StmtColumnInfo*)linitial(lidx))->relid;
        foreach(lc2, relIndexInfoList)
        {
            rIndexInfo = (RelationIndexInfo*)lfirst(lc2);
            /* 
             * If this relation is current candidate index's target relation, 
             * check each index of this relation.
             */
            if(rIndexInfo->relid == relid)
            {
                foreach(lc3, rIndexInfo->idxList)
                {
                    icolsInfo = (IndexColsInfo*)lfirst(lc3);
                    /* Exists, then drop this candidate */
                    if(is_prefix_index_existing(icolsInfo->colList, lidx))
                    {
                        indexList = foreach_delete_current(indexList, lc);
                        break;
                    }
                }
                break;
            }
        }
    }

    /* Drop all duplications, finish. */
    return indexList;
}

/*
 * Create a special index name for index recommendation use
 */
static char *
create_index_name_irec(List *colList)
{
    ListCell        *lc;
    StmtColumnInfo  *colInfo;
    int             maxLen = (NAMEDATALEN + 1) * (list_length(colList) + 2);
    char            *idxName = palloc0(maxLen); /* A special prefix */
    
    strlcat(idxName, "ir_", maxLen);
    if(((StmtColumnInfo*)linitial(colList))->rel != NULL)
        strlcat(idxName, ((StmtColumnInfo*)linitial(colList))->rel->relname, maxLen);
    foreach(lc, colList)
    {
        colInfo = (StmtColumnInfo*)lfirst(lc);
        strlcat(idxName, "_", maxLen);
        strlcat(idxName, colInfo->colName, maxLen);
    }
    
    return idxName;
}


static void
index_recommendation_with_ref(RawStmt *rawStmt, RawStmt *refStmt)
{
    RangeVar            *rel;
    RelationIndexInfo   *rindexInfo;
    IndexColsInfo       *icolsInfo;
    ColAttrInfo         *cattrInfo;

    List            *rawColList = get_columns_from_stmt(rawStmt); /* A list of StmtColumnInfo* */
    List            *refColList = get_columns_from_stmt(refStmt); /* A list of StmtColumnInfo* */
    List            *rawRelationList = get_relations_from_stmt(rawStmt); /* Relations in raw stmt, a list of RangeVar* */
    List            *refRelationList = get_relations_from_stmt(refStmt); /* Relations in ref stmt, a list of RangeVar* */
    
    List            *recIndexList = NIL; /* A list of List(StmtColumnInfo*) */
    List            *targetColList = NIL; /* Target columns in ref column list during one refered index's trial */
    List            *recIndex; /* Element of recIndexList, a list of StmtColumnInfo*, used to generate index */
    ListCell        *lc, *lc2, *lc3, *lc4;

    IndexStmt       *idxStmt;

    if(rawColList == NIL || refColList == NIL || rawRelationList == NIL || refRelationList == NIL)
        return;
    
    construct_bipartite_conn(rawColList, refColList);

    /* For each relation appeared in ref stmt */
    foreach(lc, refRelationList)
    {
        rel = lfirst_node(RangeVar, lc);
        rindexInfo = get_index_list_in_rel(rel); /* RelationIndexInfo* */
        /* For each index in this relation */
        foreach(lc2, rindexInfo->idxList)
        {
            icolsInfo = (IndexColsInfo*)lfirst(lc2); /* IndexColsInfo* */
            targetColList = NIL;
            foreach(lc3, icolsInfo->colList)
            {
                cattrInfo = (ColAttrInfo*)lfirst(lc3); /* ColAttrInfo* */
                foreach(lc4, refColList)
                {
                    StmtColumnInfo *refCol = (StmtColumnInfo*)lfirst(lc4);
                    if(refCol->relid == rindexInfo->relid
                        && refCol->attrNum == cattrInfo->attrNum
                        && strcmp(refCol->colName, cattrInfo->colName) == 0)
                    {
                        targetColList = lappend(targetColList, refCol);
                        break;
                    }
                }
            }
            /* If some column(s) in this index does not exist in reference stmt's column list, skip.
             * That is, all columns of the index do not appear in the list completely */
            if(list_length(targetColList) < list_length(icolsInfo->colList))
                break;
            
            /* Append candidate indexes */
            recIndexList = append_permutation_in_bipartite(targetColList, NIL, 
                                                    recIndexList, 0, list_length(targetColList));
            
        }
    }
    
    /* Drop indexes(duplications or existing ones) */
    recIndexList = drop_duplicate_index(recIndexList, rawRelationList);

    /* Do recommendation, generate the IndexStmt */
    foreach(lc, recIndexList)
    {
        recIndex = (List*)lfirst(lc);
        idxStmt = makeNode(IndexStmt);
        idxStmt->relation = ((StmtColumnInfo*)linitial(recIndex))->rel;
        idxStmt->accessMethod = DEFAULT_INDEX_TYPE;
        foreach(lc2, recIndex)
        {
            StmtColumnInfo  *colInfo = (StmtColumnInfo*) lfirst(lc2);
            IndexElem       *idxElem = makeNode(IndexElem);
            idxElem->name = colInfo->colName;
            idxStmt->indexParams = lappend(idxStmt->indexParams, idxElem);
        }

        idxStmt->idxname = create_index_name_irec(recIndex);
        generate_index(idxStmt);
    }
    
}

static bool
is_struct_similar_query(RawStmt *stmt1, RawStmt *stmt2)
{
    StmtFeatureVec    stmtFeature1, stmtFeature2;

    construct_stmt_feature_vector(stmt1, &stmtFeature1);
    construct_stmt_feature_vector(stmt2, &stmtFeature2);
    
    return stmt_feature_equal(&stmtFeature1, &stmtFeature2);
}


/* outer interface for index recommendation */
void 
index_recommend(RawStmt *rawStmt, const char *query)
{
    if (!IsA(rawStmt->stmt, SelectStmt) 
        // && !IsA(rawStmt->stmt, InsertStmt)
        // && !IsA(rawStmt->stmt, UpdateStmt)
        // && !IsA(rawStmt->stmt, DeleteStmt)
        )
        return;

    char        *qstr;
    RawStmt     *refStmt;
    List        *parsetreeList;
    List        *refQueryList= get_history_query(); /* List of char* */
    ListCell    *lc;

    //test code, to delete
    refQueryList = NIL;
    refQueryList = lappend(refQueryList, "select * from testtable where pid = qid;");

    foreach(lc, refQueryList)
    {
        qstr = lfirst(lc);
        parsetreeList = pg_parse_query(qstr);
        refStmt = linitial(parsetreeList);
        
        if (is_struct_similar_query(rawStmt, refStmt))
            index_recommendation_with_ref(rawStmt, refStmt);

    }

    // append_history_query(query);
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

/* Check whether the stmt needs to recommend */
bool query_not_involve_system_relation(RawStmt *rawStmt)
{
    return !stmt_contains_system_relation(rawStmt);
}

/* History queries storage interface */
void
append_history_query(const char *query)
{
    if(query != NULL)
    {
        char    *qcopy = palloc0(strlen(query) + 1);
        strlcpy(qcopy, query, strlen(query) + 1);
        history_queries = lappend(history_queries, qcopy);
    }
}

List *
get_history_query()
{
    return history_queries;
}

