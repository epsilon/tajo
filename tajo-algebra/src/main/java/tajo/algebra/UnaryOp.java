package tajo.algebra;

public abstract class UnaryOp extends RelationalOp implements Cloneable {
  RelationalOp subExpr;

  public UnaryOp(OperatorType type) {
    super(type);
  }

  public RelationalOp getSubExpr() {
    return this.subExpr;
  }

  @Override
  public Object clone() throws CloneNotSupportedException {
    UnaryOp unary = (UnaryOp) super.clone();
    unary.subExpr = (RelationalOp) (subExpr == null ? null : subExpr.clone());

    return unary;
  }
}
