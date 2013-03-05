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

public class TableSubQueryOp extends RelationOp {
  private LogicalOp op;
  public TableSubQueryOp(Integer id) {
    super(id, OpType.TableSubQuery);
  }

  public void init(LogicalOp op, String name) {
    super.init(null, name);
    this.op = op;

    this.setOutSchema(op.getOutSchema());
    this.setInSchema(op.getOutSchema());
  }

  @Override
  public void preOrder(LogicalOpVisitor visitor) {
    visitor.visit(this);
    op.preOrder(visitor);
  }

  @Override
  public void postOrder(LogicalOpVisitor visitor) {
    op.postOrder(visitor);
    visitor.visit(this);
  }

  @Override
  public String[] getPlanString() {
    return new String[] {"Subquery"};
  }
}
