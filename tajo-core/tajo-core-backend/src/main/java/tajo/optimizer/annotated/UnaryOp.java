package tajo.optimizer.annotated;


public abstract class UnaryOp extends LogicalOp implements Cloneable {
	LogicalOp childOp;

	public UnaryOp(int id, OpType type) {
		super(id, type);
	}
	
	public void setChildOp(LogicalOp child) {
		this.childOp = child;
	}
	
	public LogicalOp getChildOp() {
		return this.childOp;
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
	  UnaryOp unary = (UnaryOp) super.clone();
	  unary.childOp = (LogicalOp) (childOp == null ? null : childOp.clone());
	  
	  return unary;
	}

  @Override
  public void preOrder(LogicalOpVisitor visitor) {
    visitor.visit(this);
    childOp.preOrder(visitor);
  }

  @Override
  public void postOrder(LogicalOpVisitor visitor) {
    childOp.postOrder(visitor);
    visitor.visit(this);
  }
}
