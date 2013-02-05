package tajo.algebra;

public class LikeExpr extends BinaryExpr {
  private boolean not;
  private ColumnRefExpr column_ref;
  private Expr pattern;

  public LikeExpr(boolean not, ColumnRefExpr columnRefExpr, Expr pattern) {
    super(ExprType.Like);
    this.not = not;
    this.column_ref = columnRefExpr;
    this.pattern = pattern;
  }

  public boolean isNot() {
    return not;
  }

  public ColumnRefExpr getColumnRef() {
    return this.column_ref;
  }

  public Expr getPattern() {
    return this.pattern;
  }
}
