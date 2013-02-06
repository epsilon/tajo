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

package tajo.algebra;

import tajo.util.TUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Aggregation extends UnaryOperator {
  private List<GroupElement> groups = new ArrayList<>();

  public Aggregation() {
    super(ExprType.Aggregation);
  }

  public void addGroupSet(GroupElement group) {
    groups.add(group);
  }

  public boolean isEmptyGrouping() {
    return groups.isEmpty();
  }

  public List<GroupElement> getGroupSet() {
    return Collections.unmodifiableList(groups);
  }

  public static class GroupElement implements JsonSerializable {
    private GroupType group_type;
    private ColumnReferenceExpr[] columns;

    public GroupElement(GroupType groupType, ColumnReferenceExpr[] columns) {
      this.group_type = groupType;
      this.columns = columns;
    }

    public GroupType getType() {
      return this.group_type;
    }

    public ColumnReferenceExpr[] getColumns() {
      return this.columns;
    }

    public String toString() {
      return toJson();
    }

    @Override
    public String toJson() {
      return JsonHelper.toJson(this);
    }

    public boolean equals(Object obj) {
      if (obj instanceof GroupElement) {
        GroupElement other = (GroupElement) obj;
        return group_type.equals(other) &&
            TUtil.checkEquals(columns, other.columns);
      }

      return false;
    }
  }

  public static enum GroupType {
    GROUPBY,
    CUBE,
    ROLLUP
  }
}
