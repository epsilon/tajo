package tajo.frontend.sql;

import tajo.algebra.*;

public class QueryBlock {
  private Projection projection;

  private boolean distinct = false;
  /* select target list */
  private Target[] targetList = null;
  /* from clause or join clause */
  private Expr fromClause = null;
  /* where clause */
  private Expr whereCond = null;
  /* if true, there is at least one aggregation function. */
  private boolean aggregation = false;
  /* if true, there is at least grouping field. */
  private Aggregation groupbyClause = null;
  /* having condition */
  private Expr havingCond = null;
  /* keys for ordering */
  private Sort sort = null;
  /* limit clause */
  private Limit limitClause = null;

  public QueryBlock() {
  }

  public void setProjection(Projection projection) {
    this.projection = projection;
  }

  public void setFromClause(Expr fromClause) {
    this.fromClause = fromClause;
  }

  public void setSearchCondition(Expr expr) {
    this.whereCond = expr;
  }

  public void setGroupbyClause(Aggregation groupby) {
    this.groupbyClause = groupby;
  }

  public void setSort(Sort sort) {
    this.sort = sort;
  }
}
