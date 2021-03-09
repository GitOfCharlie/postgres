/*-------------------------------------------------------------------------
 *
 * pg_query.h
 *	  definition of the "query" system catalog (pg_query)
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_query.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_QUERY_H
#define PG_QUERY_H

#include "catalog/genbki.h"
#include "catalog/pg_query_d.h"
#include "c.h"


/* ----------------
 *		pg_query definition.  cpp turns this into
 *		typedef struct FormData_pg_query.
 * ----------------
 */
CATALOG(pg_query,4600,QueryRelationId) 
{
    int32   qid;
	/* variable-length fields start here, but we allow direct access to query */
	text	query BKI_FORCE_NOT_NULL;

#ifdef CATALOG_VARLEN
#endif
} FormData_pg_query;

/* ----------------
 *		Form_pg_query corresponds to a pointer to a tuple with
 *		the format of pg_query relation.
 * ----------------
 */
typedef FormData_pg_query *Form_pg_query;

DECLARE_UNIQUE_INDEX(pg_query_qid_index, 4601, on pg_query using btree(qid oid_ops));
#define QueryQidIndexId  4601

extern void QueryCreate(int32 qid, const char *query);
extern List *QueriesGet();

#endif