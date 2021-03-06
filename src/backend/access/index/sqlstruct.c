/*-------------------------------------------------------------------------
 *
 * sqlstruct.c
 *	  
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/stmtmatch/sqlmatch.c
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
#include "access/sqlstruct.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/catalog.h"
#include "commands/tablecmds.h"
#include "utils/lsyscache.h"
#include <string.h>


/* functions of sql struct */
static PredicateKind cast_operator_to_predicate_kind(char *op);

static bool is_aggregate_func(Node* node);
static void stat_AExpr(A_Expr* node, int *stat);
static void set_predicate_statistic(Node* node, int *stat);
static void set_num_of_aggregation(RawStmt *stmt, StmtFeatureVec *feature);
static void set_num_of_subquery(RawStmt *stmt, StmtFeatureVec *feature);

static List *tranverse_JOIN_expr_tree(Node *expr, List *relList);
static List *get_relations_from_FROM_clause(List *fromClause);
static List *get_columns_from_WHERE_clause(Node *whereCls, List *colList);
static List *get_columns_from_ORDERBY_clause(List *sortCls, List *colList, List *targetCols);
static List *get_columns_from_GROUPBY_clause(List *groupbyCls, List *colList, List *targetCols);


/* Check whether the stmt is target to system relations(catalogs) */
bool 
stmt_contains_system_relation(RawStmt *rawStmt)
{
    bool rels_contain_system_catalog = false;
    if(IsA(rawStmt->stmt, SelectStmt))
    {
        SelectStmt      *stmt = (SelectStmt*)rawStmt->stmt;
        List            *relationList = get_relations_from_FROM_clause(stmt->fromClause);
        ListCell        *lc;
        foreach(lc, relationList)
        {
            RangeVar    *relVar = lfirst_node(RangeVar, lc);
            Relation    relation = table_openrv(relVar, AccessShareLock);

            if(IsSystemRelation(relation))
            {
                rels_contain_system_catalog = true;
                table_close(relation, AccessShareLock);
                break;
            }
            table_close(relation, AccessShareLock);
        }
        return rels_contain_system_catalog;
    }
    else
    {
        // Other type of stmt ignored
        // rels_contain_system_catalog = true;
    }
    return rels_contain_system_catalog;
}

/* help functions for feature vectors */
static PredicateKind
cast_operator_to_predicate_kind(char *op)
{
    if(strcmp(op, "=") == 0)
        return PRED_EQ;
    else if(strcmp(op, "<>") == 0)
        return PRED_NEQ;
    else if(strcmp(op, ">") == 0 || strcmp(op, ">=") == 0 ||
            strcmp(op, "<") == 0 || strcmp(op, "<=") == 0)
        return PRED_COMPARE;
    return PRED_OTHER;
}

static bool
is_aggregate_func(Node* node)
{
    if(IsA(node, FuncCall))
    {
        List        *funcNames;
        ListCell    *name;
        funcNames = ((FuncCall*)node)->funcname;
        foreach(name, funcNames)
        {
            if (strcmp(strVal(lfirst(name)), "count") == 0 \
                || strcmp(strVal(lfirst(name)), "max") == 0 \
                || strcmp(strVal(lfirst(name)), "min") == 0 \
                || strcmp(strVal(lfirst(name)), "avg") == 0 \
                || strcmp(strVal(lfirst(name)), "sum") == 0)
            {
                return true;
            }
        }
    }
    return false;
}

static void
stat_AExpr(A_Expr* node, int *stat)
{
    if(!IsA(node, A_Expr))
        stat[PRED_OTHER] += 1;
    else
    {
        A_Expr      *expr = (A_Expr*)node;
        switch (expr->kind)
        {
        case AEXPR_OP: /* check >, <, >=, <= */
        {
            List        *nameList = expr->name;
            ListCell    *lc;
            foreach(lc, nameList)
            {
                char    *predName = strVal(lfirst(lc));
                stat[cast_operator_to_predicate_kind(predName)] += 1;
            }
            break;
        }
        case AEXPR_OP_ANY:
            stat[PRED_ANY] += 1;
            break;
        case AEXPR_OP_ALL:
            stat[PRED_ALL] += 1;
            break;
        case AEXPR_BETWEEN:
        case AEXPR_BETWEEN_SYM:
        case AEXPR_NOT_BETWEEN:
        case AEXPR_NOT_BETWEEN_SYM:
            stat[PRED_BETWEEN] += 1;
            break;
        default:
            stat[PRED_OTHER] += 1;
            break;
        }
        
    }
}

static void
set_predicate_statistic(Node* node, int *stat)
{
    if (node == NULL)
        return;
    
    switch (nodeTag(node))
    {
    case T_A_Expr:
    {
        A_Expr      *expr = (A_Expr*)node;
        stat_AExpr(expr, stat);
        break;
    }
    case T_BoolExpr:
    {
        BoolExpr    *bExpr = (BoolExpr*)node;
        ListCell    *lc;
        Node        *expr;
        foreach(lc, bExpr->args)
        {
            expr = (Node*)lfirst(lc);
            set_predicate_statistic(expr, stat); //recursion
        }
        break;
    }
    case T_SubLink:
    {
        SubLink     *slExpr = (SubLink*)node;
        switch (slExpr->subLinkType)
        {
        case ALL_SUBLINK:
            stat[PRED_ALL] += 1;
            break;
        case ANY_SUBLINK:
            stat[PRED_ANY] += 1;
            break;
        default:
            stat[PRED_OTHER] += 1;
            break;
        }
        break;
    }
    default:
        break;
    }

}

static void
set_num_of_aggregation(RawStmt *stmt, StmtFeatureVec *feature)
{
    if(stmt == NULL || feature == NULL)
        return;

    if(IsA(stmt->stmt, SelectStmt))
    {
        List        *targetList;
        ListCell    *lc;
        int         count = 0;

        targetList = ((SelectStmt *)stmt->stmt)->targetList;
        foreach(lc, targetList)
        {
            ResTarget* resTarget = lfirst_node(ResTarget, lc);
            if(is_aggregate_func(resTarget->val)){
                count++;
            }
        }
        feature->aggNum = count;
    }
    
}

static void
set_num_of_subquery(RawStmt *stmt, StmtFeatureVec *feature)
{
    int count = 0;

    if(stmt == NULL || feature == NULL)
        return;

    if(IsA(stmt->stmt, SelectStmt)){
        List        *fromClauseList = ((SelectStmt *)stmt->stmt)->fromClause;
        Node        *whereClauseNode = ((SelectStmt *)stmt->stmt)->whereClause;
        ListCell    *lc;
        Node        *tmpNode;
        // check number of subqueries in from clause
        if(fromClauseList != NULL)
        {
            foreach(lc, fromClauseList)
            {
                tmpNode = (Node*)lfirst(lc);
                if(IsA(tmpNode, RangeSubselect))
                    count++;
            }
        }
        // check number of subqueries in from clause
        if(whereClauseNode != NULL)
        {
            if(IsA(whereClauseNode, SubLink))
                count++;
            else if(IsA(whereClauseNode, BoolExpr))
            {
                List    *argList = ((BoolExpr*)whereClauseNode)->args;
                foreach(lc, argList)
                {
                    tmpNode = (Node*)lfirst(lc);
                    if(IsA(tmpNode, SubLink))
                        count++;
                }
            }
        }
        
    }
    feature->subQueryNum = count;

}

/* feature vector construction and equalation judgement */
void
construct_stmt_feature_vector(RawStmt *stmt, StmtFeatureVec *feature)
{
    if(stmt != NULL && feature != NULL)
    {
        /* init feature vector*/
        MemSet((void*)feature, 0, sizeof(StmtFeatureVec));

        feature->sqlType = stmt->stmt->type;
        if(IsA(stmt->stmt, SelectStmt))
        {
            feature->tableNum = list_length(((SelectStmt *)stmt->stmt)->fromClause);
            set_predicate_statistic(((SelectStmt *)stmt->stmt)->whereClause, feature->predicateNums);
            set_num_of_aggregation(stmt, feature);

            feature->hasGroupBy = (((SelectStmt *)stmt->stmt)->groupClause != NULL);
            feature->hasOrderBy = (((SelectStmt *)stmt->stmt)->sortClause != NULL);
            feature->hasLimit = (((SelectStmt *)stmt->stmt)->limitCount != NULL);
            feature->hasOffset = (((SelectStmt *)stmt->stmt)->limitOffset != NULL);
            feature->hasDistinct = (((SelectStmt *)stmt->stmt)->distinctClause != NULL);
            
            //TODO
            set_num_of_subquery(stmt, feature);
        }
    }

}

bool
stmt_feature_equal(StmtFeatureVec *feature1, StmtFeatureVec *feature2)
{
    if(feature1->sqlType == feature2->sqlType && \
        feature1->tableNum == feature2->tableNum && \
        feature1->aggNum == feature2->aggNum && \
        feature1->hasGroupBy == feature2->hasGroupBy && \
        feature1->hasOrderBy == feature2->hasOrderBy && \
        feature1->hasLimit == feature2->hasLimit && \
        feature1->hasOffset == feature2->hasOffset && \
        feature1->hasDistinct == feature2->hasDistinct )
    {
        for(int i = 0;i < PRED_KIND_NUM;i++)
        {
            if(feature1->predicateNums[i] != feature2->predicateNums[i])
                return false;
        }
        return true;
    }

    return false;
}


/* Functions for getting all relations/columns in stmt */

/* 
 * Get relations in FROM clause (RangeVar*)
 */
static List *
tranverse_JOIN_expr_tree(Node *expr, List *relList)
{
    if(expr == NULL)
        return relList;

    if(IsA(expr, RangeVar))
    {
        relList = lappend(relList, (RangeVar*)expr);
        return relList;
    }
    if(IsA(expr, JoinExpr))
    {
        JoinExpr *joinExpr = castNode(JoinExpr, expr);
        relList = tranverse_JOIN_expr_tree(joinExpr->larg, relList);
        relList = tranverse_JOIN_expr_tree(joinExpr->rarg, relList);
        return relList;
    }
    return relList;
}

static List *
get_relations_from_FROM_clause(List *fromClause)
{
    List            *fromTables = NIL;
    ListCell        *lc;
    foreach(lc, fromClause)
    {
        if(IsA(lfirst(lc), RangeVar))
        {
            RangeVar *relation = lfirst_node(RangeVar, lc);
            fromTables = lappend(fromTables, relation);
        }else if(IsA(lfirst(lc), JoinExpr))
        {
            JoinExpr *joinExpr = lfirst_node(JoinExpr, lc);
            fromTables = tranverse_JOIN_expr_tree((Node*)joinExpr, fromTables);
        }
    }
    return fromTables;
}

/* 
 * Get columns in WHERE ORDERBY GROUPBY clause
 */
static List *
get_columns_from_WHERE_clause(Node *whereCls, List *colList)
{
    switch (nodeTag(whereCls))
    {
    case T_A_Expr:
    {
        A_Expr          *expr = (A_Expr*)whereCls;
        PredicateKind   predKind;
        switch (expr->kind)
        {
        case AEXPR_OP: /* check >, <, >=, <= */
        {
            char    *predName = strVal(linitial(expr->name));
            predKind = cast_operator_to_predicate_kind(predName);
            break;
        }
        case AEXPR_OP_ANY:
            predKind = PRED_ANY;
            break;
        case AEXPR_OP_ALL:
            predKind = PRED_ALL;
            break;
        case AEXPR_BETWEEN:
        case AEXPR_BETWEEN_SYM:
        case AEXPR_NOT_BETWEEN:
        case AEXPR_NOT_BETWEEN_SYM:
            predKind = PRED_BETWEEN;
            break;
        default:
            predKind = PRED_OTHER;
            break;
        }
        if(IsA(expr->lexpr, ColumnRef))
        {
            StmtColumnInfo *colInfo = palloc0(sizeof(StmtColumnInfo));
            colInfo->colRef = (ColumnRef*)expr->lexpr;
            set_from_predicate(colInfo, predKind);
            colList = lappend(colList, colInfo);
        }
        if(IsA(expr->rexpr, ColumnRef))
        {
            StmtColumnInfo *colInfo = palloc0(sizeof(StmtColumnInfo));;
            colInfo->colRef = (ColumnRef*)expr->rexpr;
            set_from_predicate(colInfo, predKind);
            colList = lappend(colList, colInfo);
        }
        break;
    }
    case T_BoolExpr:
    {
        BoolExpr    *bExpr = (BoolExpr*)whereCls;
        ListCell    *lc;
        Node        *expr;
        foreach(lc, bExpr->args)
        {
            expr = (Node*)lfirst(lc);
            colList = get_columns_from_WHERE_clause(expr, colList); //recursion
        }
        break;
    }
    case T_SubLink:
    {
        SubLink     *slExpr = (SubLink*)whereCls;
        Node        *lefthand = slExpr->testexpr;
        
        if(IsA(lefthand, ColumnRef))
        {
            StmtColumnInfo *colInfo = palloc0(sizeof(StmtColumnInfo));
            colInfo->colRef = (ColumnRef*)lefthand;
            switch (slExpr->subLinkType)
            {
            case ALL_SUBLINK:
                set_from_predicate(colInfo, PRED_ALL);
                break;
            case ANY_SUBLINK:
                set_from_predicate(colInfo, PRED_ANY);
                break;
            default:
                set_from_predicate(colInfo, PRED_OTHER);
                break;
            }
            colList = lappend(colList, colInfo);
        }
        break;
    }
    default:
        break;
    }

    return colList;
}

static List *
get_columns_from_ORDERBY_clause(List *sortCls, List *colList, List *targetCols)
{
    ListCell    *lc;
    foreach(lc, sortCls)
    {
        SortBy      *sortBy = lfirst_node(SortBy, lc);
        if(IsA(sortBy->node, ColumnRef)) // order by colname
        {
            StmtColumnInfo *colInfo = palloc0(sizeof(StmtColumnInfo));
            colInfo->colRef = (ColumnRef*)sortBy->node;
            set_from_predicate(colInfo, PRED_ORDERBY);
            colList = lappend(colList, colInfo);
        }else if(IsA(sortBy->node, A_Const)) // order by 1, 2
        {
            int colIdx = intVal(&((A_Const*)sortBy->node)->val) - 1; // e.g. order by 1 means the 0th column
            if(colIdx < list_length(targetCols)) // avoid select * ...group by 1, 2
            {
                ResTarget *colTarget = list_nth_node(ResTarget, targetCols, colIdx);
                if(IsA(colTarget->val, ColumnRef))
                {
                    StmtColumnInfo *colInfo = palloc0(sizeof(StmtColumnInfo));
                    colInfo->colRef = (ColumnRef*)colTarget->val;
                    set_from_predicate(colInfo, PRED_ORDERBY);
                    colList = lappend(colList, colInfo);
                }
            }
        }
    }
    return colList;
}

static List *
get_columns_from_GROUPBY_clause(List *groupbyCls, List *colList, List *targetCols)
{
    ListCell    *lc;
    foreach(lc, groupbyCls)
    {
        Node        *node = (Node*)lfirst(lc);
        if(IsA(node, ColumnRef))
        {
            StmtColumnInfo *colInfo = palloc0(sizeof(StmtColumnInfo));
            colInfo->colRef = (ColumnRef*)node;
            set_from_predicate(colInfo, PRED_GROUPBY);
            colList = lappend(colList, colInfo);
        }else if(IsA(node, A_Const)) // group by 1, 2
        {
            int colIdx = intVal(&((A_Const*)node)->val) - 1; // e.g. group by 1 means the 0th column
            if(colIdx < list_length(targetCols)) // avoid select * ...group by 1, 2
            {
                ResTarget *colTarget = list_nth_node(ResTarget, targetCols, colIdx);
                if(IsA(colTarget->val, ColumnRef))
                {
                    StmtColumnInfo *colInfo = palloc0(sizeof(StmtColumnInfo));
                    colInfo->colRef = (ColumnRef*)colTarget->val;
                    set_from_predicate(colInfo, PRED_GROUPBY);
                    colList = lappend(colList, colInfo);
                }
            }
        }
    }
    return colList;
}

/* 
 * Get useful column infos from atomic predicates 
 * and GROUPBY, ORDERBY clause (a list of StmtColumnInfo) 
 */
List*
get_columns_from_stmt(RawStmt *rawStmt)
{
    List            *colList = NIL; // List of StmtColumnInfo
    List            *fromTables = NIL; // Tables in FROM clause, List of RangeVar
    ListCell        *lc, *lc2;

    if(IsA(rawStmt->stmt, SelectStmt))
    {
        SelectStmt      *stmt = (SelectStmt*)rawStmt->stmt;
        Node            *whereCls = stmt->whereClause;
        List            *sortCls = stmt->sortClause;
        List            *groupbyCls = stmt->groupClause;
        List            *targetCols = stmt->targetList;

        // get columns in WHERE clause
        if(whereCls != NULL)
        {
            colList = get_columns_from_WHERE_clause(whereCls, colList);
        }
        // get columns in ORDER BY clause
        if(sortCls != NULL)
        {
            // foreach(lc, sortCls)
            // {
            //     SortBy      *sortBy = lfirst_node(SortBy, lc);
            //     if(IsA(sortBy->node, ColumnRef))
            //     {
            //         StmtColumnInfo *colInfo = palloc0(sizeof(StmtColumnInfo));
            //         colInfo->colRef = (ColumnRef*)sortBy->node;
            //         set_from_predicate(colInfo, PRED_ORDERBY);
            //         colList = lappend(colList, colInfo);
            //     }
            // }
            colList = get_columns_from_ORDERBY_clause(sortCls, colList, targetCols);
        }
        // get columns in GROUP BY clause
        if(groupbyCls != NULL)
        {
            // foreach(lc, groupbyCls)
            // {
            //     Node        *node = (Node*)lfirst(lc);
            //     if(IsA(node, ColumnRef))
            //     {
            //         StmtColumnInfo *colInfo = palloc0(sizeof(StmtColumnInfo));
            //         colInfo->colRef = (ColumnRef*)node;
            //         set_from_predicate(colInfo, PRED_GROUPBY);
            //         colList = lappend(colList, colInfo);
            //     }
            // }
            colList = get_columns_from_GROUPBY_clause(groupbyCls, colList, targetCols);
        }

        // fromTables = stmt->fromClause;
        fromTables = get_relations_from_FROM_clause(stmt->fromClause);
    }

    if(colList == NIL || fromTables == NIL)
        return colList;

    // After getting all colInfos...
    foreach(lc, colList)
    {
        StmtColumnInfo  *stmtColumnInfo = (StmtColumnInfo*) lfirst(lc);
        List	        *fields = stmtColumnInfo->colRef->fields;
        
        switch (list_length(fields))
        {
        case 1: /* format: col */
        {
            stmtColumnInfo->colName = strVal(linitial(fields));
            foreach(lc2, fromTables)
            {
                if(IsA(lfirst(lc2), RangeVar)) // a simple table
                {
                    RangeVar    *rel = lfirst_node(RangeVar, lc2);
                    Oid         relid = RangeVarGetRelidExtended(rel, ShareLock,
                                                                RVR_MISSING_OK,
                                                                RangeVarCallbackOwnsRelation,
                                                                NULL);
                    AttrNumber  attrNum = get_attnum(relid, stmtColumnInfo->colName);
                    if(relid != InvalidOid && attrNum != InvalidAttrNumber)
                    {
                        stmtColumnInfo->rel = rel;
                        stmtColumnInfo->relid = relid;
                        stmtColumnInfo->attrNum = attrNum;
                        break;
                    }
                }
            }
            break;
        }
        case 2: /* format: rel.col */
        {
            char    *relAlias = strVal(linitial(fields));
            stmtColumnInfo->colName = strVal(lsecond(fields));
            foreach(lc2, fromTables)
            {
                if(IsA(lfirst(lc2), RangeVar)) // a simple table
                {
                    RangeVar    *rel = lfirst_node(RangeVar, lc2);
                    Oid         relid = RangeVarGetRelidExtended(rel, ShareLock,
                                                                RVR_MISSING_OK,
                                                                RangeVarCallbackOwnsRelation,
                                                                NULL);
                    AttrNumber  attrNum = get_attnum(relid, stmtColumnInfo->colName);
                    /* if 1st field equals relation's alias or name, and the column exists */
                    if((strcmp(relAlias, rel->relname) == 0 
                        || (rel->alias != NULL && strcmp(relAlias, rel->alias->aliasname) == 0))
                        && relid != InvalidOid && attrNum != InvalidAttrNumber)
                    {
                        stmtColumnInfo->rel = rel;
                        stmtColumnInfo->relid = relid;
                        stmtColumnInfo->attrNum = attrNum;
                        break;
                    }
                }
            }
            break;
        }
        default:
            break;
        }

        if(stmtColumnInfo->rel == NULL)
        {
            colList = foreach_delete_current(colList, lc);
            // pfree(stmtColumnInfo);
        }
    }

    if(colList == NIL)
        return colList;

    /* Combine duplicated columns into one struct */
    foreach(lc, colList)
    {
        StmtColumnInfo  *currInfo = (StmtColumnInfo*) lfirst(lc);
        if(lnext(colList, lc) != NULL)
        {
            /* loop from the next cell to combine duplicate ones afterwards */
            for_each_cell(lc2, colList, lnext(colList, lc))
            {
                StmtColumnInfo  *laterInfo = (StmtColumnInfo*) lfirst(lc2);
                /* If same relation and same column name, combine them */
                if(currInfo->rel == laterInfo->rel && strcmp(currInfo->colName, laterInfo->colName) == 0)
                {
                    combine_predicate(laterInfo, currInfo);
                    colList = foreach_delete_current(colList, lc2);
                    /* Don't need to decrease lc__state since lc2 is ahead of lc*/
                }
            }
        }
        
    }
    return colList;
}

/* 
 * Group the columns by relation 
 * List colListByRelation: Each element of it is a list of columns belonging to a certain relation
 * Param allColList: List(StmtColumnInfo*)
 */
List *
group_columns_by_relation(List *allColList)
{
    List            *colListByRelation = NIL; /* Each element of it is a list contains columns list in one certain relation */
    List            *colList = NIL, *newList = NIL; /* Temp vars for loop traversal */
    ListCell        *lc, *lc2;
    StmtColumnInfo  *currColInfo, *groupedColInfo;
    RangeVar        *relation;
    bool            relationExists = false;
    
    if(allColList == NIL)
        return colListByRelation;

    foreach(lc, allColList)
    {
        currColInfo = (StmtColumnInfo*) lfirst(lc);
        relation = currColInfo->rel;
        relationExists = false;
        foreach(lc2, colListByRelation)
        {
            colList = (List*)lfirst(lc2);
            groupedColInfo = (StmtColumnInfo*)linitial(colList);
            /* If the relation current column belongs to has been grouped */
            if(currColInfo->rel == groupedColInfo->rel)
            {
                relationExists = true;
                colList = lappend(colList, currColInfo);
                break;
            }
        }
        /* Not grouped yet, create a new List into colListByRelation */
        if(!relationExists)
        {
            newList = list_make1(currColInfo);
            colListByRelation = lappend(colListByRelation, newList);
        }
    }

    return colListByRelation;
}

/* 
 * Get all relations' info involved in a stmt (a list of RangeVar) 
 */
List*
get_relations_from_stmt(RawStmt *rawStmt)
{
    if(IsA(rawStmt->stmt, SelectStmt))
    {
        SelectStmt  *stmt = rawStmt->stmt;
        return get_relations_from_FROM_clause(stmt->fromClause);
    }

    return NIL;
}