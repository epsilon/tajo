package tajo.algebra;

public class LikeExpr extends Expr {
  private boolean not;
  private ColumnRef column_ref;
  private Expr pattern;

  public LikeExpr(boolean not, ColumnRef columnRef, Expr pattern) {
    super(ExpressionType.Like);
    this.not = not;
    this.column_ref = columnRef;
    this.pattern = pattern;
  }

  public boolean isNot() {
    return not;
  }

  public ColumnRef getColumnRef() {
    return this.column_ref;
  }

  public Expr getPattern() {
    return this.pattern;
  }
}
