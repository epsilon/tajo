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
import tajo.algebra.*;

import static junit.framework.Assert.*;

public class TestSQLAnalyzer {
  private static SQLAnalyzer analyzer = null;

  @BeforeClass
  public static void setup() {
    analyzer = new SQLAnalyzer();
  }

  @Test
  public void testCreateEvalTree() throws Exception {
    SQLAnalyzer analyzer = new SQLAnalyzer();
//    Expr expr = analyzer.createExpression(
//        TestSQLParser.parseExpr(TestSQLParser.arithmeticExprs[5]).value_expression().tree);
  }

  @Test
  public void testCreateEvalTreeComp() throws Exception {
    SQLAnalyzer analyzer = new SQLAnalyzer();

    CommonTree tree =
        TestSQLParser.parseExpr(TestSQLParser.comparisonExpr[0]).boolean_value_expression().tree;
    System.out.println(tree.toStringTree());
    //Expr expr = analyzer.createExpression(tree);

    //System.out.println(expr.toString());
  }

  @Test
  public void testCreateEvalTreeLogic() throws Exception {
    SQLAnalyzer analyzer = new SQLAnalyzer();

    CommonTree tree =
        TestSQLParser.parseExpr(TestSQLParser.comparisonExpr[0]).boolean_value_expression().tree;
    System.out.println(tree.toStringTree());
    //Expr expr = analyzer.createExpression(tree);

    //System.out.println(expr.toString());
  }

  private String[] QUERIES = {
      "select id, name, score, age from people", // 0
      "select name, score, age from people where score > 30", // 1
      "select name, score, age from people where 3 + 5 * 3", // 2
      "select age, sumtest(score) as total from people group by age having sumtest(score) > 30", // 3
      "select p.id, s.id, score, dept from people as p, student as s where p.id = s.id", // 4
      "select name, score from people order by score asc, age desc null first", // 5
      // only expr
      "select 7 + 8 as total", // 6
      // limit test
      "select id, name, score, age from people limit 3" // 7
  };

  @Test
  public final void testSelectStatement() {
    Expr expr = analyzer.parse(QUERIES[0]);
    assertEquals(ExprType.Projection, expr.getType());
    Projection projection = (Projection) expr;
    assertEquals(ExprType.Selection, projection.getChild().getType());
    Selection selection = (Selection) projection.getChild();
    assertEquals(ExprType.Relation, selection.getChild().getType());
    Relation relation = (Relation) selection.getChild();
    assertEquals("people", relation.getName());
  }

  @Test
  public final void testSelectStatementWithAlias() {
    Expr expr = analyzer.parse(QUERIES[4]);
    assertEquals(ExprType.Projection, expr.getType());
    Projection projection = (Projection) expr;
    assertEquals(ExprType.Selection, projection.getChild().getType());
    Selection selection = (Selection) projection.getChild();
    assertEquals(ExprType.Join, selection.getChild().getType());
    Join join = (Join) selection.getChild();
    assertEquals(ExprType.Relation, join.getLeft().getType());
    Relation outer = (Relation) join.getLeft();
    assertEquals("p", outer.getAlias());
    assertEquals(ExprType.Relation, join.getRight().getType());
    Relation inner = (Relation) join.getRight();
    assertEquals("s", inner.getAlias());
  }

  @Test
  public final void testOrderByClause() {
    Expr block = analyzer.parse(QUERIES[5]);
    testOrderByCluse(block);
  }

  @Test
  public final void testOnlyExpr() {
    Expr expr = analyzer.parse(QUERIES[6]);
    assertEquals(ExprType.Projection, expr.getType());
    Projection projection = (Projection) expr;
    assertEquals(1, projection.getTargets().length);
    Target target = projection.getTargets()[0];
    assertEquals("total", target.getAlias());
    assertEquals(ExprType.Plus, target.getExpr().getType());
  }

  @Test
  public void testLimit() {
    Expr expr = analyzer.parse(QUERIES[7]);
    assertEquals(ExprType.Projection, expr.getType());
    Projection projection = (Projection) expr;
    assertEquals(ExprType.Limit, projection.getChild().getType());
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
        "select case when p_type like 'PROMO%' then l_extendedprice * (1 - l_discount) " +
            "when p_type = 'MOCC' then l_extendedprice - 100 else 0 end as cond from lineitem, part");

    String json = tree.toJson();

    Expr recover = JsonHelper.fromJson(json, Expr.class);
    System.out.println(recover);
  }

  public static String [] subQueries = {
      "select c1, c2, c3 from (select c1, c2, c3 from employee) as test",
      "select c1, c2, c3 from table1 where c3 < (select c4 from table2)"
  };

  @Test
  public void testTableSubQuery() {
    Expr algebra = analyzer.parse(subQueries[0]);
    System.out.println(algebra.toJson());
  }

  @Test
  public void testScalarSubQuery() {
    Expr algebra = analyzer.parse(subQueries[1]);
    System.out.println(algebra.toJson());
  }

  static final String [] setQualifier = {
      "select id, people_id from student",
      "select distinct id, people_id from student",
      "select all id, people_id from student",
  };

  @Test
  public final void testSetQulaifier() {
    Expr expr = analyzer.parse(setQualifier[0]);
    assertEquals(ExprType.Projection, expr.getType());
    Projection projection = (Projection) expr;
    assertFalse(projection.isDistinct());

    expr = analyzer.parse(setQualifier[1]);
    assertEquals(ExprType.Projection, expr.getType());
    projection = (Projection) expr;
    assertTrue(projection.isDistinct());

    expr = analyzer.parse(setQualifier[2]);
    assertEquals(ExprType.Projection, expr.getType());
    projection = (Projection) expr;
    assertFalse(projection.isDistinct());
  }

  static final String [] createTableStmts = {
      "create table table1 (name varchar, age int)",
      "create table table1 (name string, age int) using rcfile",
      "create table table1 (name string, age int) using rcfile with ('rcfile.buffer' = 4096)",
      // create table test
      "create table store1 as select name, score from people order by score asc, age desc null first",// 0
      // create table test
      "create table store1 (c1 string, c2 long) as select name, score from people order by score asc, age desc null first",// 1
      // create table test
      "create table store2 using rcfile with ('rcfile.buffer' = 4096) as select name, score from people order by score asc, age desc null first", // 2
      // create table def
      "create table table1 (name string, age int, earn long, score float) using rcfile with ('rcfile.buffer' = 4096)", // 4
      // create table def with location
      "create external table table1 (name string, age int, earn long, score float) using csv with ('csv.delimiter'='|') location '/tmp/data'" // 5
  };

  @Test
  public final void testCreateTable1() {
    CreateTable stmt = (CreateTable) analyzer.parse(createTableStmts[0]);
    assertEquals("table1", stmt.getRelationName());
    assertTrue(stmt.hasTableElements());

    stmt = (CreateTable) analyzer.parse(createTableStmts[1]);
    assertEquals("table1", stmt.getRelationName());
    assertTrue(stmt.hasTableElements());
    assertTrue(stmt.hasStorageType());
    assertEquals("rcfile", stmt.getStorageType());

    stmt = (CreateTable) analyzer.parse(createTableStmts[2]);
    assertEquals("table1", stmt.getRelationName());
    assertTrue(stmt.hasTableElements());
    assertTrue(stmt.hasStorageType());
    assertEquals("rcfile", stmt.getStorageType());
    assertTrue(stmt.hasParams());
    assertEquals("4096", stmt.getParams().get("rcfile.buffer"));
  }

  @Test
  public final void testCreateTableAsSelect() {
    CreateTable stmt = (CreateTable) analyzer.parse(createTableStmts[3]);
    assertEquals("store1", stmt.getRelationName());
    assertTrue(stmt.hasSubQuery());
    testOrderByCluse(stmt.getSubQuery());

    stmt = (CreateTable) analyzer.parse(createTableStmts[4]);
    assertEquals("store1", stmt.getRelationName());
    assertTrue(stmt.hasSubQuery());
    testOrderByCluse(stmt.getSubQuery());
    assertTrue(stmt.hasTableElements());

    stmt = (CreateTable) analyzer.parse(createTableStmts[5]);
    assertEquals("store2", stmt.getRelationName());
    assertEquals("rcfile", stmt.getStorageType());
    assertEquals("4096", stmt.getParams().get("rcfile.buffer"));
    testOrderByCluse(stmt.getSubQuery());
  }

  private static void testOrderByCluse(Expr block) {
    Projection projection = (Projection) block;
    Sort sort = (Sort) projection.getChild();

    assertEquals(2, sort.getSortSpecs().length);
    Sort.SortSpec spec1 = sort.getSortSpecs()[0];
    assertEquals("score", spec1.getKey().getColumnName());
    assertEquals(true, spec1.isAscending());
    assertEquals(false, spec1.isNullFirst());
    Sort.SortSpec spec2 = sort.getSortSpecs()[1];
    assertEquals("age", spec2.getKey().getColumnName());
    assertEquals(false, spec2.isAscending());
    assertEquals(true, spec2.isNullFirst());
  }

  @Test
  public final void testCreateTableDef1() {
    CreateTable stmt = (CreateTable) analyzer.parse(createTableStmts[6]);
    assertEquals("table1", stmt.getRelationName());
    CreateTable.ColumnDefinition[] elements = stmt.getTableElements();
    assertEquals("name", elements[0].getColumnName());
    assertEquals("string", elements[0].getDataType());
    assertEquals("age", elements[1].getColumnName());
    assertEquals("int", elements[1].getDataType());
    assertEquals("earn", elements[2].getColumnName());
    assertEquals("long", elements[2].getDataType());
    assertEquals("score", elements[3].getColumnName());
    assertEquals("float", elements[3].getDataType());
    assertEquals("rcfile", stmt.getStorageType());
    assertFalse(stmt.hasLocation());
    assertTrue(stmt.hasParams());
    assertEquals("4096", stmt.getParams().get("rcfile.buffer"));
  }

  @Test
  public final void testCreateTableDef2() {
    CreateTable expr = (CreateTable) analyzer.parse(createTableStmts[7]);
    _testCreateTableDef2(expr);
    CreateTable restored = (CreateTable) AlgebraTestingUtil.testJsonSerializer(expr);
    _testCreateTableDef2(restored);
  }

  private void _testCreateTableDef2(CreateTable expr) {
    assertEquals("table1", expr.getRelationName());
    CreateTable.ColumnDefinition[] elements = expr.getTableElements();
    assertEquals("name", elements[0].getColumnName());
    assertEquals("string", elements[0].getDataType());
    assertEquals("age", elements[1].getColumnName());
    assertEquals("int", elements[1].getDataType());
    assertEquals("earn", elements[2].getColumnName());
    assertEquals("long", elements[2].getDataType());
    assertEquals("score", elements[3].getColumnName());
    assertEquals("float", elements[3].getDataType());
    assertEquals("csv", expr.getStorageType());
    assertEquals("/tmp/data", expr.getLocation());
    assertTrue(expr.hasParams());
    assertEquals("|", expr.getParams().get("csv.delimiter"));
  }
}
