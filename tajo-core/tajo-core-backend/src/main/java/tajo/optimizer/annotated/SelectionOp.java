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

import tajo.engine.eval.EvalNode;

import java.util.ArrayList;
import java.util.List;

public class SelectionOp extends UnaryOp implements Cloneable {

	public SelectionOp(Integer id) {
		super(id, OpType.Selection);
	}

  public void init(EvalNode [] cnf) {
    setQual(cnf);
  }

	public EvalNode [] getQual() {
		return (EvalNode []) getAnnotation("qual");
	}

	public void setQual(EvalNode [] cnf) {
		putAnnotation("qual", cnf);
	}
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SelectionOp) {
      SelectionOp other = (SelectionOp) obj;
      return super.equals(other)
          && childOp.equals(other.childOp);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    SelectionOp selNode = (SelectionOp) super.clone();
    
    return selNode;
  }

  @Override
  public String[] getPlanString() {
    List<String> strings = new ArrayList<String>();

    strings.add("Selection");
    StringBuilder sb = new StringBuilder("Search Cond: ");
    EvalNode [] predicates = getQual();
    for (int i = 0; i < predicates.length; i++) {
      sb.append(predicates[i].toString());
      if( i < predicates.length - 1) {
        sb.append(" AND ");
      }
    }
    strings.add(sb.toString());

    return strings.toArray(new String[strings.size()]);
  }
}
