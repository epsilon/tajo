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

public class TableSubQueryOp extends LogicalOp {
  private LogicalOp op;
  private String rel_name;
  public TableSubQueryOp(Integer id) {
    super(id, OpType.TableSubQuery);
  }


  public void init(LogicalOp op, String name) {
    this.op = op;
    this.rel_name = name;
  }

  public String getName() {
    return rel_name;
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
