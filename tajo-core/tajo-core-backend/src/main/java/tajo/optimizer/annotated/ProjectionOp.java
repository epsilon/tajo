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

import com.google.gson.annotations.Expose;
import tajo.engine.json.GsonCreator;
import tajo.engine.parser.QueryBlock.Target;
import tajo.engine.planner.logical.LogicalNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ProjectionOp extends UnaryOp {
  /**
   * the targets are always filled even if the query is 'select *'
   */
  @Expose	private Target [] targets;
  @Expose private boolean distinct = false;

  /**
   * This method is for gson.
   */
	public ProjectionOp(Integer id) {
		super(id, OpType.Projection);
	}

	public void init(Target[] targets) {
		this.targets = targets;
	}
	
	public Target [] getTargets() {
	  return this.targets;
	}

  public void setTargetList(Target [] targets) {
    this.targets = targets;
  }

  @Override
  public List<String> getPlanString() {
    List<String> strings = new ArrayList<String>();
    StringBuilder sb = new StringBuilder();
    sb.append("Projection: ");
    if (distinct) {
      sb.append(" (distinct)");
    }
    strings.add(sb.toString());

    sb = new StringBuilder("Targets: ");
    for (int i = 0; i < targets.length; i++) {
      sb.append(targets[i]);
      if( i < targets.length - 1) {
        sb.append(",");
      }
    }
    strings.add(sb.toString());
    if (getOutSchema() != null) {
      strings.add("  out schema: " + getOutSchema().toString());
    }
    if (getInSchema() != null) {
      strings.add("  in  schema: " + getInSchema().toString());
    }
    return strings;
  }
	
	@Override
  public boolean equals(Object obj) {
	  if (obj instanceof ProjectionOp) {
	    ProjectionOp other = (ProjectionOp) obj;
	    
	    boolean b1 = super.equals(other);
	    boolean b2 = Arrays.equals(targets, other.targets);
	    boolean b3 = childOp.equals(other.childOp);
	    
	    return b1 && b2 && b3;
	  } else {
	    return false;
	  }
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
	  ProjectionOp projNode = (ProjectionOp) super.clone();
	  projNode.targets = targets.clone();
	  
	  return projNode;
	}
	
	public String toJSON() {
	  return GsonCreator.getInstance().toJson(this, LogicalNode.class);
	}
}
