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

import java.util.ArrayList;
import java.util.List;

public class RelationListOp extends LogicalOp {
  private RelationOp [] relations;

  public RelationListOp(Integer id) {
    super(id, OpType.RelationList);
  }

  public void init(RelationOp [] relations) {
    this.relations = relations;
  }

  public RelationOp [] getRelations() {
    return this.relations;
  }

  @Override
  public void preOrder(LogicalOpVisitor visitor) {
    visitor.visit(this);
    for (LogicalOp relation : relations) {
      relation.preOrder(visitor);
    }
  }

  @Override
  public void postOrder(LogicalOpVisitor visitor) {
    for (LogicalOp relation : relations) {
      relation.postOrder(visitor);
    }
    visitor.visit(this);
  }

  @Override
  public String[] getPlanString() {
    List<String> strings = new ArrayList<String>();
    strings.add("Relation List");

    StringBuilder sb = new StringBuilder("List: ");
    for (int i = 0; i < relations.length; i++) {
      RelationOp relationOp = (RelationOp) relations[i];
      sb.append(relationOp.getName());

      if (relationOp.hasAlias()) {
        sb.append(" as " + relationOp.getAlias());
      }
      if( i < relations.length - 1) {
        sb.append(",");
      }
    }

    strings.add(sb.toString());
    return strings.toArray(new String[strings.size()]);
  }
}
