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

import tajo.catalog.Column;
import tajo.engine.eval.EvalNode;
import tajo.engine.parser.QueryBlock;
import tajo.util.TUtil;

import java.util.Arrays;
import java.util.List;

public class AggregationOp extends UnaryOp implements Cloneable {
	private QueryBlock.GroupElement [] groups;
	private EvalNode havingCondition = null;
	private QueryBlock.Target[] targets;

	public AggregationOp(Integer id) {
		super(id, OpType.Aggregation);
	}

  public void init(QueryBlock.GroupElement[] groups, QueryBlock.Target [] targets ) {
    this.groups = groups;
    this.targets = targets;
  }
	
	public final QueryBlock.GroupElement[] getGroups() {
	  return this.groups;
	}
	
	public final boolean hasHavingCondition() {
	  return this.havingCondition != null;
	}
	
	public final EvalNode getHavingCondition() {
	  return this.havingCondition;
	}
	
	public final void setHavingCondition(final EvalNode evalTree) {
	  this.havingCondition = evalTree;
	}
	
  public boolean hasTargetList() {
    return this.targets != null;
  }

  public QueryBlock.Target[] getTargets() {
    return this.targets;
  }

  public void setTargetList(QueryBlock.Target[] targets) {
    this.targets = targets;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AggregationOp) {
      AggregationOp other = (AggregationOp) obj;
      return super.equals(other) 
          && Arrays.equals(groups, other.groups)
          && TUtil.checkEquals(havingCondition, other.havingCondition)
          && TUtil.checkEquals(targets, other.targets)
          && childOp.equals(other.childOp);
    } else {
      return false;  
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    AggregationOp grp = (AggregationOp) super.clone();
    if (groups != null) {
      grp.groups = new QueryBlock.GroupElement[groups.length];
      for (int i = 0; i < groups.length; i++) {
        grp.groups[i] = (QueryBlock.GroupElement) groups[i].clone();
      }
    }
    grp.havingCondition = (EvalNode) (havingCondition != null 
        ? havingCondition.clone() : null);    
    if (targets != null) {
      grp.targets = new QueryBlock.Target[targets.length];
      for (int i = 0; i < targets.length; i++) {
        grp.targets[i] = (QueryBlock.Target) targets[i].clone();
      }
    }

    return grp;
  }

  @Override
  public List<String> getPlanString() {
    List<String> strings = TUtil.newList();
    strings.add("Aggregation");

    StringBuilder sb = new StringBuilder("Targets: ");
    for (int i = 0; i < targets.length; i++) {
      sb.append(targets[i]);
      if( i < targets.length - 1) {
        sb.append(",");
      }
    }
    strings.add(sb.toString());

    sb = new StringBuilder("Groups: ");
    for (int i = 0; i < groups.length; i++) {
      sb.append(groups[i].getType().toString());

      sb.append("(");

      Column [] groupingColumns = groups[i].getColumns();
      for (int j = 0; j < groupingColumns.length; j++) {
        sb.append(groupingColumns[i].getColumnName());
        if(j < groupingColumns.length - 1) {
          sb.append(",");
        }
      }
      sb.append(")");
      if( i < groups.length - 1) {
        sb.append(",");
      }
    }

    strings.add(sb.toString());
    return strings;
  }
}
