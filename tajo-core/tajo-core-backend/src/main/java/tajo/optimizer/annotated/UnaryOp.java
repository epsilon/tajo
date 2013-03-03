/**
 * 
 */
package tajo.optimizer.annotated;

import com.google.gson.annotations.Expose;


/**
 * @author Hyunsik Choi
 *
 */
public abstract class UnaryOp extends LogicalOp implements Cloneable {
	@Expose LogicalOp child;

	public UnaryOp(int id, OpType type) {
		super(id, type);
	}
	
	public void setChild(LogicalOp child) {
		this.child = child;
	}
	
	public LogicalOp getSubNode() {
		return this.child;
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
	  UnaryOp unary = (UnaryOp) super.clone();
	  unary.child = (LogicalOp) (child == null ? null : child.clone());
	  
	  return unary;
	}

  @Override
  public void preOrder(LogicalOpVisitor visitor) {
    visitor.visit(this);
    child.preOrder(visitor);
  }

  @Override
  public void postOrder(LogicalOpVisitor visitor) {
    child.postOrder(visitor);
    visitor.visit(this);
  }
}
