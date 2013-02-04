package tajo.algebra;

import tajo.catalog.Schema;

public abstract class RelationalOp implements Cloneable {
	protected OperatorType type;
	protected Schema inputSchema;
	protected Schema outputSchema;

	public RelationalOp(OperatorType type) {
		this.type = type;
	}
	
	public OperatorType getType() {
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
	  if (obj instanceof RelationalOp) {
	    RelationalOp other = (RelationalOp) obj;

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
	  RelationalOp node = (RelationalOp)super.clone();
	  node.type = type;
	  node.inputSchema = 
	      (Schema) (inputSchema != null ? inputSchema.clone() : null);
	  node.outputSchema = 
	      (Schema) (outputSchema != null ? outputSchema.clone() : null);
	  
	  return node;
	}
}
