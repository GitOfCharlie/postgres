/*-------------------------------------------------------------------------
 *
 * indexrec.h
 *	  index recommendation.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/indexrec.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef INDEXREC_H
#define INDEXREC_H

#include "nodes/parsenodes.h"

extern void index_recommend(RawStmt *rawStmt, const char *query);
extern void index_recommend_simple(RawStmt *rawStmt);
extern bool query_not_involve_system_relation(RawStmt *rawStmt);

extern void append_history_query(int32 qid, const char *query);
extern List *get_history_query();

#endif