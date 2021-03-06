/*-------------------------------------------------------------------------
 *
 * sqlstruct.h
 *	  help functions of sql struct judgement for index recommendation.
 *
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/sqlstruct.h
 *
 *-------------------------------------------------------------------------
 */

#ifndef SQLSTRUCT_H
#define SQLSTRUCT_H

#include "nodes/parsenodes.h"
#include "fmgr.h"

#define PRED_KIND_NUM 		7 /* for struct similarity */
#define PRED_KIND_NUM_INCLUDE_CLAUSE 9 /* for all kinds of pred + group by + order by */
typedef enum PredicateKind
{
	PRED_EQ = 0,
	PRED_NEQ,
	PRED_COMPARE,
	PRED_BETWEEN,
	PRED_ANY,
	PRED_ALL,
	PRED_OTHER,

    PRED_GROUPBY,
    PRED_ORDERBY
} PredicateKind;

/* for sql struct computation */
typedef struct StmtFeatureVec
{
	// don't need tag in this struct : NodeTag			type;
	
	NodeTag			sqlType;
	int				tableNum;
	int				predicateNums[PRED_KIND_NUM];
	int				aggNum;
	bool			hasGroupBy;
	bool			hasOrderBy;
	bool			hasLimit;
	bool			hasOffset;
	bool			hasDistinct;

	// List			*subqueryVecSet;
	int				subQueryNum;

}StmtFeatureVec;

/*
 * Column info in stmt
 */
typedef struct StmtColumnInfo
{
    RangeVar        *rel;       /* the relation reference */
	Oid				relid;

	ColumnRef       *colRef;
    char            *colName;   /* column name */
	AttrNumber		attrNum;

	bits16			predicateFlag; /* this column is from which predicate(s)? */
	/* 
	 * In index recommend algo.'s bipartite,
	 * connectCol is the column list, the elements of which is connected with this column 
	 */
	List			*connectCol;
	
}StmtColumnInfo;

static inline void
set_from_predicate(StmtColumnInfo *scInfo, PredicateKind predKind)
{
	Assert(scInfo != NULL);
	scInfo->predicateFlag |= (1 << predKind);
}

static inline bool
is_from_predicate(StmtColumnInfo *scInfo, PredicateKind predKind)
{
	Assert(scInfo != NULL);
	return (scInfo->predicateFlag & (1 << predKind)) != 0;
}

static inline void
combine_predicate(StmtColumnInfo *srcInfo, StmtColumnInfo *destInfo)
{
	Assert(srcInfo != NULL && destInfo != NULL);
	destInfo->predicateFlag |= srcInfo->predicateFlag;
}

static inline bool
has_common_from_predicate(StmtColumnInfo *scInfo1, StmtColumnInfo *scInfo2)
{
	Assert(scInfo1 != NULL && scInfo2 != NULL);
	return (scInfo1->predicateFlag & scInfo2->predicateFlag) != 0;
}

/* functions of sql struct */
extern bool stmt_contains_system_relation(RawStmt *rawStmt);

extern void construct_stmt_feature_vector(RawStmt *stmt, StmtFeatureVec *feature);
extern bool stmt_feature_equal(StmtFeatureVec *feature1, StmtFeatureVec *feature2);

extern List *get_columns_from_stmt(RawStmt *rawStmt);
extern List *group_columns_by_relation(List *allColList);
extern List *get_relations_from_stmt(RawStmt *rawStmt);

#endif