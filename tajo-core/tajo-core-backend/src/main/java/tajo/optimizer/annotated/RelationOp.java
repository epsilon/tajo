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
import tajo.catalog.Schema;
import tajo.catalog.TableMeta;
import tajo.engine.eval.EvalNode;
import tajo.engine.json.GsonCreator;
import tajo.engine.parser.QueryBlock.Target;
import tajo.util.TUtil;

public class RelationOp extends LogicalOp {
	@Expose private String rel_name;
  @Expose private TableMeta meta;
  @Expose private String alias;
	@Expose private EvalNode qual;
	@Expose private Target[] targets;

  protected RelationOp(Integer id, OpType type) {
    super(id, type);
  }

  @SuppressWarnings("unused")
	public RelationOp(Integer id) {
		super(id, OpType.Relation);
	}

  public void init(TableMeta meta, String relationName, String alias) {
    init(meta, relationName);
    this.alias = alias;
  }

  public void init(TableMeta meta, String relationName) {
    this.meta = meta;
    rel_name = relationName;
    setInSchema(meta.getSchema());
    setOutSchema(meta.getSchema());
  }
	
	public String getName() {
	  return rel_name;
	}

  public TableMeta getMeta() {
    return meta;
  }

  public Schema getSchema() {
    return meta.getSchema();
  }
	
	public boolean hasAlias() {
	  return alias != null;
	}

  public String getAlias() {
    return alias;
  }

  public String getRelationId() {
    if (alias != null) {
      return alias;
    } else {
      return rel_name;
    }
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
	
	public String toJSON() {
	  return GsonCreator.getInstance().toJson(this, LogicalOp.class);
	}

  @Override
  public int hashCode() {
    return Objects.hashCode(rel_name, alias, qual, targets);
  }
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof RelationOp) {
	    RelationOp other = (RelationOp) obj;
	    
	    boolean eq = super.equals(other); 
	    eq = eq && TUtil.checkEquals(rel_name, other.rel_name);
      eq = eq && TUtil.checkEquals(alias, other.alias);
	    eq = eq && TUtil.checkEquals(this.qual, other.qual);
	    eq = eq && TUtil.checkEquals(this.targets, other.targets);
	    
	    return eq;
	  }	  
	  
	  return false;
	}	
	
	@Override
	public Object clone() throws CloneNotSupportedException {
	  RelationOp scanNode = (RelationOp) super.clone();
	  
	  scanNode.rel_name = this.rel_name;
    scanNode.alias = this.alias;
	  
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

  @Override
  public void preOrder(LogicalOpVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public void postOrder(LogicalOpVisitor visitor) {
    visitor.visit(this);
  }

  @Override
  public String[] getPlanString() {
    StringBuilder sb = new StringBuilder("Scan on " + rel_name);
    if (hasAlias()) {
      sb.append(" as " + alias);
    }
    return new String[] {sb.toString()};
  }
}
