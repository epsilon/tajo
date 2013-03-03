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

package tajo.optimizer.annotated;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import tajo.algebra.Relation;
import tajo.catalog.Schema;
import tajo.engine.eval.EvalNode;
import tajo.engine.json.GsonCreator;
import tajo.engine.parser.QueryBlock.FromTable;
import tajo.engine.parser.QueryBlock.Target;
import tajo.engine.planner.logical.ExprType;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.logical.LogicalNodeVisitor;
import tajo.util.TUtil;

public class RelationOp extends LogicalOp {
	@Expose private FromTable table;
	@Expose private EvalNode qual;
	@Expose private Target[] targets;

	public RelationOp(Integer id) {
		super(id, OpType.Relation);
	}

  public void init(Relation relation, Schema schema) {
    table = new FromTable(relation.getName(), schema);
    if (relation.hasAlias()) {
      table.setAlias(relation.getAlias());
    }
    setInSchema(schema);
    setOutSchema(schema);
  }
	
	public String getTableId() {
	  return table.getTableName();
	}
	
	public boolean hasAlias() {
	  return table.hasAlias();
	}
	
	public boolean hasQual() {
	  return qual != null;
	}
	
	public EvalNode getQual() {
	  return this.qual;
	}
	
	public void setQual(EvalNode evalTree) {
	  this.qual = evalTree;
	}
	
	public boolean hasTargetList() {
	  return this.targets != null;
	}
	
	public void setTargets(Target [] targets) {
	  this.targets = targets;
	}
	
	public Target [] getTargets() {
	  return this.targets;
	}
	
	public String toString() {
	  return toJSON();
	}
	
	public String toJSON() {
	  return GsonCreator.getInstance().toJson(this, LogicalOp.class);
	}

  @Override
  public int hashCode() {
    return Objects.hashCode(this.table, this.qual, this.targets);
  }
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof RelationOp) {
	    RelationOp other = (RelationOp) obj;
	    
	    boolean eq = super.equals(other); 
	    eq = eq && TUtil.checkEquals(this.table, other.table);
	    eq = eq && TUtil.checkEquals(this.qual, other.qual);
	    eq = eq && TUtil.checkEquals(this.targets, other.targets);
	    
	    return eq;
	  }	  
	  
	  return false;
	}	
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  RelationOp scanNode = (RelationOp) super.clone();
	  
	  scanNode.table = (FromTable) this.table.clone();
	  
	  if (hasQual()) {
	    scanNode.qual = (EvalNode) this.qual.clone();
	  }
	  
	  if (hasTargetList()) {
	    scanNode.targets = new Target[targets.length];
      for (int i = 0; i < targets.length; i++) {
        scanNode.targets[i] = (Target) targets[i].clone();
      }
	  }
	  
	  return scanNode;
	}
}
