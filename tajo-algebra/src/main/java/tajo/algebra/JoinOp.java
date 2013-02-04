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

import tajo.engine.planner.JoinType;

public class JoinOp extends BinaryOp {
  private JoinType join_type;
  private Expr join_qual;
  private ColumnRef [] joinColumns;
  private boolean natural = false;

  public JoinOp(JoinType joinType) {
    super(OperatorType.Join);
    this.join_type = joinType;
  }

  public JoinOp(JoinType joinType, Relation right) {
    this.outer = right;
  }

  public JoinOp(Expr join_qual, RelationalOp outer, RelationalOp inner) {
    super(OperatorType.Join, outer, inner);
    this.join_qual = join_qual;
  }

  public JoinType getJoinType() {
    return  this.join_type;
  }

  public boolean hasQual() {
    return this.join_qual != null;
  }

  public Expr getQual() {
    return this.join_qual;
  }

  public void setJoinQual(Expr expr) {
    this.join_qual = expr;
  }

  public void setJoinColumns(ColumnRef [] columns) {
    joinColumns = columns;
  }

  public boolean hasLeftJoin() {
    return false;
  }

  public void setNatural() {
    natural = true;
  }

  public boolean isNatural() {
    return natural;
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }
}
