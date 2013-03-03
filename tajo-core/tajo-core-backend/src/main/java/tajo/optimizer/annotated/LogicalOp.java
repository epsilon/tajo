/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 
 */
package tajo.optimizer.annotated;

import com.google.gson.annotations.Expose;
import tajo.catalog.Schema;
import tajo.engine.planner.logical.ExprType;

import java.util.Map;

/**
 * Logical Expression with Annotations
 */
public abstract class LogicalOp implements Cloneable {
  int id;
  @Expose private OpType type;
	@Expose private Schema inputSchema;
	@Expose private Schema outputSchema;
  Map<String, Object> annotations;
	
	protected LogicalOp(Integer id, OpType type) {
    this.id = id;
    this.type = type;
	}

	protected LogicalOp(OpType type) {
		this.type = type;
	}

  public int getId() {
    return this.id;
  }
	
	public OpType getType() {
		return this.type;
	}

	public void setType(OpType type) {
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
	  if (obj instanceof LogicalOp) {
	    LogicalOp other = (LogicalOp) obj;

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
	  LogicalOp node = (LogicalOp)super.clone();
	  node.type = type;
	  node.inputSchema = 
	      (Schema) (inputSchema != null ? inputSchema.clone() : null);
	  node.outputSchema = 
	      (Schema) (outputSchema != null ? outputSchema.clone() : null);
	  
	  return node;
	}
	
	public abstract String toJSON();
}
