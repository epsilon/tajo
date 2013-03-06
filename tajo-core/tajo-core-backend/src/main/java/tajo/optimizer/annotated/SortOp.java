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

import com.google.common.base.Preconditions;
import tajo.catalog.SortSpec;
import tajo.util.TUtil;

import java.util.ArrayList;
import java.util.List;

public final class SortOp extends UnaryOp implements Cloneable {
	private SortSpec[] sortKeys;

  @SuppressWarnings("unused")
  public SortOp(Integer id) {
    super(id, OpType.SORT);
  }

  public void init(SortSpec[] sortKeys) {
    Preconditions.checkArgument(sortKeys.length > 0, "at least one sort key must be specified");
    this.sortKeys = sortKeys;
  }
  
  public SortSpec[] getSortKeys() {
    return this.sortKeys;
  }
  
  @Override 
  public boolean equals(Object obj) {
    if (obj instanceof SortOp) {
      SortOp other = (SortOp) obj;
      return super.equals(other)
          && TUtil.checkEquals(sortKeys, other.sortKeys)
          && childOp.equals(other.childOp);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    SortOp sort = (SortOp) super.clone();
    sort.sortKeys = sortKeys.clone();
    
    return sort;
  }

  @Override
  public List<String> getPlanString() {
    List<String> strings = new ArrayList<String>();
    strings.add("Sort");
    StringBuilder sb = new StringBuilder("Sort Keys: ");
    for (int i = 0; i < sortKeys.length; i++) {
      sb.append(sortKeys[i].getKey().getColumnName()).append(" ")
          .append(sortKeys[i].isAscending() ? "asc" : "desc");
      if( i < sortKeys.length - 1) {
        sb.append(",");
      }
    }
    strings.add(sb.toString());

    return strings;
  }
}
