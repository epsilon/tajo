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

public class RelationListOp extends LogicalOp {
  LogicalOp [] relations;

  public RelationListOp(Integer id) {
    super(id, OpType.RelationList);
  }

  public void init(LogicalOp [] relations) {
    this.relations = relations;
  }

  public LogicalOp [] getRelations() {
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
}
