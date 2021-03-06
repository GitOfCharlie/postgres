/*-------------------------------------------------------------------------
 *
 * indexinfo.h
 *	  help functions of achieving index infomation in db.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/indexinfo.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef INDEXINFO_H
#define INDEXINFO_H

#include "nodes/parsenodes.h"
#include "postgres_ext.h"
#include "nodes/pg_list.h"

/*
 * All indexes info in relation
 */
typedef struct RelationIndexInfo
{
    Oid         relid;
    List        *idxList; /* List of IndexColsInfo */
} RelationIndexInfo;

/*
 * All columns info in an index
 */
typedef struct IndexColsInfo
{
    Oid         idxid;
    List        *colList; /* List of ColAttrInfo */
} IndexColsInfo;

/*
 * A column in a index
 */
typedef struct ColAttrInfo
{
    AttrNumber  attrNum;
    char        *colName;
} ColAttrInfo;


extern RelationIndexInfo *get_index_list_in_rel(RangeVar *rel);


#endif