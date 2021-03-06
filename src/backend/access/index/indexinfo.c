/*-------------------------------------------------------------------------
 *
 * indexinfo.c
 *	  
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
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

#include "access/genam.h"
#include "access/indexinfo.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_index.h"
#include "commands/tablecmds.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"


RelationIndexInfo *
get_index_list_in_rel(RangeVar *rel)
{
    Oid                 relid, idxid;
    RelationIndexInfo   *relationIndexInfo;
    IndexColsInfo       *indexColsInfo;
    ColAttrInfo         *colAttrInfo;

    Relation            relation, indexRel;
    List                *indexList = NIL; /* A list of OIDs of indexes on this relation */
    ListCell            *lc;

    if(rel == NULL)
        return NULL;
    
    /* Get relation Oid */
    relid = RangeVarGetRelidExtended(rel, ShareLock,
                                    RVR_MISSING_OK,
                                    RangeVarCallbackOwnsRelation,
                                    NULL);
    if(relid == InvalidOid)
        return NULL;
    
    relationIndexInfo = palloc0(sizeof(RelationIndexInfo));
    relationIndexInfo->relid = relid;
    relation = table_open(relid, AccessShareLock);

    /* Get all indexes' oid */
    indexList = RelationGetIndexList(relation);
    foreach(lc, indexList)
    {
        idxid = lfirst_oid(lc);

        indexColsInfo = palloc0(sizeof(IndexColsInfo));
        indexColsInfo->idxid = idxid;

        indexRel = index_open(idxid, AccessShareLock);
        Form_pg_index   indexStruct = indexRel->rd_index;
        int16		    indnatts = indexStruct->indnatts;
        /* Set all columns info of one index */
        for(int16 i = 0;i < indexStruct->indnkeyatts;i++)
        {
            colAttrInfo = palloc0(sizeof(ColAttrInfo));
            colAttrInfo->attrNum = indexStruct->indkey.values[i];
            colAttrInfo->colName = get_attname(relid, colAttrInfo->attrNum, false);
            indexColsInfo->colList = lappend(indexColsInfo->colList, colAttrInfo);
        }
        
        relationIndexInfo->idxList = lappend(relationIndexInfo->idxList, indexColsInfo);

        index_close(indexRel, AccessShareLock);
    }

    table_close(relation, AccessShareLock);

    return relationIndexInfo;
}