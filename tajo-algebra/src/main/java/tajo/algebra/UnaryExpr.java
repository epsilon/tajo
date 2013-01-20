package tajo.algebra;

public abstract class UnaryExpr extends RelationalExpr implements Cloneable {
  RelationalExpr subExpr;

  public UnaryExpr(ExprType type) {
    super(type);
  }

  public void setSubExpr(RelationalExpr expr) {
    this.subExpr = expr;
  }

  public RelationalExpr getSubExpr() {
    return this.subExpr;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    UnaryExpr unary = (UnaryExpr) super.clone();
    unary.subExpr = (RelationalExpr) (subExpr == null ? null : subExpr.clone());

    return unary;
  }
}
