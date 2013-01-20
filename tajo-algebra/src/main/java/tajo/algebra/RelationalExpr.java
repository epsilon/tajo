package tajo.algebra;

import com.google.gson.annotations.Expose;
import tajo.catalog.Schema;

public abstract class RelationalExpr implements Cloneable {
	protected ExprType type;
	protected Schema inputSchema;
	protected Schema outputSchema;

	public RelationalExpr(ExprType type) {
		this.type = type;
	}
	
	public ExprType getType() {
		return this.type;
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
	  if (obj instanceof RelationalExpr) {
	    RelationalExpr other = (RelationalExpr) obj;

      boolean b1 = this.type == other.type;
      boolean b2 = this.inputSchema.equals(other.inputSchema);
      boolean b3 = this.outputSchema.equals(other.outputSchema);
      
      return b1 && b2 && b3;
	  } else {
	    return false;
	  }
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  RelationalExpr node = (RelationalExpr)super.clone();
	  node.type = type;
	  node.inputSchema = 
	      (Schema) (inputSchema != null ? inputSchema.clone() : null);
	  node.outputSchema = 
	      (Schema) (outputSchema != null ? outputSchema.clone() : null);
	  
	  return node;
	}
}
