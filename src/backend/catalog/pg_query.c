/*-------------------------------------------------------------------------
 *
 * pg_query.c
 *	  routines to support manipulation of the pg_query relation
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/catalog/pg_query.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/catalog.h"
#include "catalog/indexing.h"
#include "catalog/pg_query.h"
#include "utils/builtins.h"
// #include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

void  
QueryCreate(int32 qid, const char *query)
{
    Relation        qrel;
    HeapTuple	    tuple;
    text            *qtext = cstring_to_text(query);
    Datum           values[Natts_pg_query];
    bool            nulls[Natts_pg_query];


    qrel = table_open(QueryRelationId, RowExclusiveLock);

    /* Fill nulls and values */
    MemSet(nulls, false, sizeof(nulls));

    values[Anum_pg_query_qid - 1] = Int32GetDatum(qid);
    values[Anum_pg_query_query - 1] = PointerGetDatum(qtext);
    
    tuple = heap_form_tuple(RelationGetDescr(qrel), values, nulls);

	/* Insert the tuple into the pg_query catalog */
	CatalogTupleInsert(qrel, tuple);

    /* Close the relation and free the tuple */
    table_close(qrel, RowExclusiveLock);
    heap_freetuple(tuple);
}

List *
QueriesGet()
{
    Relation        qrel;
    SysScanDesc     qsscan;
    HeapTuple	    tuple;
    Form_pg_query   query;
    char            *qstr;
    List            *qList = NIL;

    /* Scan pg_query systable */
    qrel = table_open(QueryRelationId, AccessShareLock);
    qsscan = systable_beginscan(qrel, InvalidOid, false,
							   NULL, 0, NULL);
    
    while(HeapTupleIsValid(tuple = systable_getnext(qsscan)))
    {
        query = (Form_pg_query) GETSTRUCT(tuple);
        qstr = text_to_cstring(&query->query);

        qList = lappend(qList, qstr);
    }

    /* End of scan */
    systable_endscan(qsscan);
    table_close(qrel, AccessShareLock);

    return qList;
}

