/**
 * 
 */
package tajo.engine.planner.logical;

import com.google.gson.annotations.Expose;
import tajo.catalog.Schema;

/**
 * Logical Expression with Annotations
 */
public abstract class LogicalNode implements Cloneable {
	@Expose
	private ExprType type;
	@Expose
	private Schema inputSchema;
	@Expose
	private Schema outputSchema;

	@Expose
	private double cost = 0;
	
	public LogicalNode() {
		
	}

	public LogicalNode(ExprType type) {
		this.type = type;
	}
	
	public ExprType getType() {
		return this.type;
	}

	public void setType(ExprType type) {
		this.type = type;
	}
	
	public void setInSchema(Schema inSchema) {
	  this.inputSchema = inSchema;
	}
	
	public Schema getInSchema() {
	  return this.inputSchema;
	}
	
	public void setOutSchema(Schema outSchema) {
	  this.outputSchema = outSchema;
	}
	
	public Schema getOutSchema() {
	  return this.outputSchema;
	}
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof LogicalNode) {
	    LogicalNode other = (LogicalNode) obj;

      boolean b1 = this.type == other.type;
      boolean b2 = this.inputSchema.equals(other.inputSchema);
      boolean b3 = this.outputSchema.equals(other.outputSchema);
      boolean b4 = this.cost == other.cost;
      
      return b1 && b2 && b3 && b4;
	  } else {
	    return false;
	  }
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  LogicalNode node = (LogicalNode)super.clone();
	  node.type = type;
	  node.inputSchema = 
	      (Schema) (inputSchema != null ? inputSchema.clone() : null);
	  node.outputSchema = 
	      (Schema) (outputSchema != null ? outputSchema.clone() : null);
	  
	  return node;
	}
	
	public abstract String toJSON();

	public abstract void preOrder(LogicalNodeVisitor visitor);
  public abstract void postOrder(LogicalNodeVisitor visitor);
}
