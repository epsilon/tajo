package tajo.algebra;

public abstract class UnaryOperator extends Expr {
  private Expr child;

  @SuppressWarnings("unused")
  UnaryOperator() {}

  public UnaryOperator(ExprType type) {
    super(type);
  }

  public Expr getChild() {
    return this.child;
  }

  public void setChild(Expr op) {
    this.child = op;
  }
}
