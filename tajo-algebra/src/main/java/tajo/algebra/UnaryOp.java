package tajo.algebra;

public abstract class UnaryOp extends RelationalOp {
  private RelationalOp child;

  @SuppressWarnings("unused")
  UnaryOp() {}

  public UnaryOp(OperatorType type) {
    super(type);
  }

  public RelationalOp getSubOp() {
    return this.child;
  }

  public void setSubOp(RelationalOp op) {
    this.child = op;
  }
}
