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

import org.junit.Test;

import java.util.Stack;

public class TestExprVisitor {
  @Test
  public void test() {
    Expr expr = new BinaryOperator(ExprType.LessThan,
        new LiteralExpr("1", LiteralExpr.LiteralType.Unsigned_Integer),
        new LiteralExpr("2", LiteralExpr.LiteralType.Unsigned_Integer));

    Relation relation = new Relation("employee");
    Selection selection = new Selection(relation, expr);

    Aggregation aggregation = new Aggregation();
    aggregation.setTargets(new Target[]{
        new Target(new ColumnReferenceExpr("col1"))
    });

    aggregation.setChild(selection);

    Sort.SortSpec spec = new Sort.SortSpec(new ColumnReferenceExpr("col2"));
    Sort sort = new Sort(new Sort.SortSpec[]{spec});
    sort.setChild(aggregation);

    ExprVisitor visitor = new ExprVisitor() {
      @Override
      public void visit(Expr expr) {
        System.out.println(expr.getType());
      }
    };

    sort.accept(visitor);
  }
}
