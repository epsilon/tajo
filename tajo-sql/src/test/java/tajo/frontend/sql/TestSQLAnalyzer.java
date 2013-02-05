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

package tajo.frontend.sql;

import org.antlr.runtime.tree.CommonTree;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.algebra.Expr;
import tajo.algebra.JsonHelper;

public class TestSQLAnalyzer {
  private static SQLAnalyzer analyzer = null;

  @BeforeClass
  public static void setup() {
    analyzer = new SQLAnalyzer();
  }

  @Test
  public void testCreateEvalTree() throws Exception {
    SQLAnalyzer analyzer = new SQLAnalyzer();
    Expr expr = analyzer.createExpression(
        TestSQLParser.parseExpr(TestSQLParser.arithmeticExprs[5]).value_expression().tree);
  }

  @Test
  public void testCreateEvalTreeComp() throws Exception {
    SQLAnalyzer analyzer = new SQLAnalyzer();

    CommonTree tree =
        TestSQLParser.parseExpr(TestSQLParser.comparisonExpr[0]).boolean_value_expression().tree;
    System.out.println(tree.toStringTree());
    Expr expr = analyzer.createExpression(tree);

    System.out.println(expr.toString());
  }

  @Test
  public void testCreateEvalTreeLogic() throws Exception {
    SQLAnalyzer analyzer = new SQLAnalyzer();

    CommonTree tree =
        TestSQLParser.parseExpr(TestSQLParser.comparisonExpr[0]).boolean_value_expression().tree;
    System.out.println(tree.toStringTree());
    Expr expr = analyzer.createExpression(tree);

    System.out.println(expr.toString());
  }

  static String [] JOINS = {
      "select p.id, name, branch_name from people as p natural join student natural join branch", // 0
      "select name, dept from people as p inner join student as s on p.id = s.people_id", // 1
      "select name, dept from people as p inner join student as s using (p.id)", // 2
      "select p.id, name, branch_name from people as p cross join student cross join branch", // 3
      "select p.id, dept from people as p left outer join student as s on p.id = s.people_id", // 4
      "select p.id, dept from people as p right outer join student as s on p.id = s.people_id", // 5
      "select p.id, dept from people as p join student as s on p.id = s.people_id", // 6
      "select p.id, dept from people as p left join student as s on p.id = s.people_id", // 7
      "select p.id, dept from people as p right join student as s on p.id= s.people_id", // 8
      "select * from table1 " +
          "cross join table2 " +
          "join table3 on table1.id = table3.id " +
          "inner join table4 on table1.id = table4.id " +
          "left outer join table5 on table1.id = table5.id " +
          "right outer join table6 on table1.id = table6.id " +
          "full outer join table7 on table1.id = table7.id " +
          "natural join table8 " +
          "natural inner join table9 " +
          "natural left outer join table10 " +
          "natural right outer join table11 " +
          "natural full outer join table12 ", // 9 - all possible join clauses*/
      "select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment, ps_supplycost " + // 10
          "from region join nation on n_regionkey = r_regionkey and r_name = 'EUROPE' " +
          "join supplier on s_nationekey = n_nationkey " +
          "join partsupp on s_suppkey = ps_ps_suppkey " +
          "join part on p_partkey = ps_partkey and p_type like '%BRASS' and p_size = 15"

  };

  @Test
  /**
   *           join
   *          /    \
   *       join    branch
   *    /       \
   * people  student
   */
  public final void testNaturalJoinClause() {
    Expr algebra = analyzer.parse(JOINS[0]);

    System.out.println(algebra);

    //JoinOp join = block.getJoinClause();
//    assertEquals(JoinType.INNER, join.getJoinType());
//    assertTrue(join.isNatural());
//    assertEquals("branch", ((Relation)join.getOuter()).getName());
//    assertTrue(join.hasLeftJoin());
//    assertEquals("people", join.getLeftJoin().getLeft().getTableName());
//    assertEquals("student", join.getLeftJoin().getRight().getTableName());
  }

  @Test
  /**
   *       join
   *    /       \
   * people student
   */
  public final void testInnerJoinClause() {
    Expr algebra = analyzer.parse(JOINS[1]);
    System.out.println(algebra);
  }

  @Test
  public final void testCrossJoinClause() {
    Expr algebra = analyzer.parse(JOINS[3]);
    System.out.println(algebra);
  }

  @Test
  public final void testLeftOuterJoinClause() {
    Expr algebra = analyzer.parse(JOINS[4]);
    System.out.println(algebra);
  }

  @Test
  public final void testRightOuterJoinClause() {
    Expr algebra = analyzer.parse(JOINS[5]);
    System.out.println(algebra);
  }

  @Test
  public final void testLeftJoinClause() {
    Expr algebra = analyzer.parse(JOINS[7]);
    System.out.println(algebra);
  }

  @Test
  public final void testRightJoinClause() {
    Expr algebra = analyzer.parse(JOINS[8]);
    System.out.println(algebra);
  }

  @Test
  public final void testAllPossibleJoins() {
    Expr algebra = analyzer.parse(JOINS[9]);
    System.out.println(algebra);
  }

  @Test
  public final void testTPCHJoin() {
    Expr algebra = analyzer.parse(JOINS[10]);
    System.out.println(algebra);
  }

  @Test
  public void testCaseWhen() {
    Expr tree = analyzer.parse(
        "select case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) "+
            "when p_type = 'MOCC' then l_extendedprice - 100 else 0 end as cond from lineitem, part");

    String json = tree.toJson();

    Expr recover = (Expr) JsonHelper.fromJson(json, Expr.class);
    System.out.println(recover);
  }
}
