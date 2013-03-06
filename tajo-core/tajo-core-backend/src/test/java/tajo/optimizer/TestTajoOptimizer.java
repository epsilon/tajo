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

package tajo.optimizer;

import org.apache.hadoop.fs.Path;
import org.junit.BeforeClass;
import org.junit.Test;
import tajo.TajoTestingCluster;
import tajo.algebra.Expr;
import tajo.benchmark.TPCH;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos;
import tajo.catalog.statistics.TableStat;
import tajo.frontend.sql.SQLAnalyzer;
import tajo.frontend.sql.SQLSyntaxError;
import tajo.master.TajoMaster;
import tajo.optimizer.annotated.LogicalPlan;
import tajo.util.FileUtil;

import java.io.File;
import java.io.IOException;

public class TestTajoOptimizer {
  private static TajoTestingCluster util;
  private static TPCH tpch;
  private static CatalogService catalog;

  @BeforeClass
  public static void setup() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.registerFunction(funcDesc);
    }

    // TPC-H Schema for Complex Queries
    String [] tpchTables = {
        "part", "supplier", "partsupp", "nation", "region", "lineitem", "customer", "orders"
    };
    int [] tableVolumns = {
        100, 200, 50, 5, 5, 800, 300, 100
    };
    tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadOutSchema();

    for (int i = 0; i < tpchTables.length; i++) {
      TableMeta m = TCatUtil.newTableMeta(tpch.getSchema(tpchTables[i]), CatalogProtos.StoreType.CSV);
      TableStat stat = new TableStat();
      stat.setNumBytes(tableVolumns[i]);
      m.setStat(stat);
      TableDesc d = TCatUtil.newTableDesc(tpchTables[i], m, new Path("file:///"));
      catalog.addTable(d);
    }
  }

  @Test
  public void test() throws SQLSyntaxError, OptimizationException {

  }

  @Test
  public void testVerify() throws SQLSyntaxError, OptimizationException {
    SQLAnalyzer sqlAnalyzer = new SQLAnalyzer();
    Expr expr = sqlAnalyzer.parse("select * from part, supplier, partsupp, nation, region, lineitem");
    System.out.println(expr);

    TajoOptimizer optimizer = new TajoOptimizer(catalog);

    //optimizer.transform(expr);
  }

  @Test
  public void testSubQuery() throws SQLSyntaxError, VerifyException {
    SQLAnalyzer sqlAnalyzer = new SQLAnalyzer();
    Expr expr = sqlAnalyzer.parse(
        "select l_orderkey from lineitem l inner join (select p_partkey from part)  p on l.l_partkey = p.partkey");

    System.out.println(expr);

    TajoOptimizer optimizer = new TajoOptimizer(catalog);

    optimizer.optimize(expr);
  }

  @Test
  public void testSort() throws SQLSyntaxError, VerifyException {
    SQLAnalyzer sqlAnalyzer = new SQLAnalyzer();
    Expr expr = sqlAnalyzer.parse(
        "select l_orderkey from lineitem l order by l_orderkey desc");

    System.out.println(expr);

    TajoOptimizer optimizer = new TajoOptimizer(catalog);

    LogicalPlan optimized = optimizer.optimize(expr);
    System.out.println(optimized);
  }

  @Test
  public void testGroupBy() throws SQLSyntaxError, VerifyException {
    SQLAnalyzer sqlAnalyzer = new SQLAnalyzer();
    Expr expr = sqlAnalyzer.parse(
        "select l_orderkey, l_partkey from lineitem group by l_orderkey, l_partkey");
    System.out.println(expr);
    TajoOptimizer optimizer = new TajoOptimizer(catalog);
    LogicalPlan optimized = optimizer.optimize(expr);
    System.out.println(optimized);
  }

  @Test
  public void testJoin() throws SQLSyntaxError, VerifyException {
    SQLAnalyzer sqlAnalyzer = new SQLAnalyzer();
    Expr expr = sqlAnalyzer.parse(
        "select l_orderkey, o_orderdate, o_shippriority from customer, orders, lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15'");

    System.out.println(expr);

    TajoOptimizer optimizer = new TajoOptimizer(catalog);

    LogicalPlan plan = optimizer.optimize(expr);

    System.out.println(plan.toString());
  }

  @Test
  public void testTPCH10Join() throws SQLSyntaxError, VerifyException {
    SQLAnalyzer sqlAnalyzer = new SQLAnalyzer();
    Expr expr = sqlAnalyzer.parse(
        "select c_custkey, c_name, c_acctbal, n_name, c_address, c_phone, c_comment from customer, orders, lineitem, nation where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate >= '1993-10-01' and o_orderdate < '1994-01-01' and l_returnflag = 'R' and c_nationkey = n_nationkey");

    System.out.println(expr);

    TajoOptimizer optimizer = new TajoOptimizer(catalog);

    LogicalPlan plan = optimizer.optimize(expr);

    System.out.println(plan.toString());
  }

  @Test
  public void testTPCHQ1() throws SQLSyntaxError, VerifyException, IOException {
    SQLAnalyzer sqlAnalyzer = new SQLAnalyzer();
    Expr expr = sqlAnalyzer.parse(FileUtil.readTextFile(new File("benchmark/tpch/q1.tql")));

    System.out.println(expr);

    TajoOptimizer optimizer = new TajoOptimizer(catalog);

    LogicalPlan plan = optimizer.optimize(expr);

    System.out.println(plan.toString());
  }

  @Test
  public void testTPCHQ2Join() throws SQLSyntaxError, VerifyException {
    SQLAnalyzer sqlAnalyzer = new SQLAnalyzer();
    Expr expr = sqlAnalyzer.parse("select s_acctbal, s_name, n_name, p_partkey, p_mfgr, s_address, s_phone, s_comment from part,supplier,partsupp,nation,region where p_partkey = ps_partkey and s_suppkey = ps_suppkey and p_size = 15 and p_type like '%BRASS' and s_nationkey = n_nationkey and n_regionkey = r_regionkey and r_name = 'c'");

    System.out.println(expr);

    TajoOptimizer optimizer = new TajoOptimizer(catalog);

    LogicalPlan plan = optimizer.optimize(expr);

    System.out.println(plan.toString());
  }

  @Test
  public void testTPCHQ3Join() throws SQLSyntaxError, VerifyException {
    SQLAnalyzer sqlAnalyzer = new SQLAnalyzer();
    Expr expr = sqlAnalyzer.parse("select l_orderkey, o_orderdate, o_shippriority from customer,orders,lineitem where c_mktsegment = 'BUILDING' and c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '1995-03-15' and l_shipdate > '1995-03-15'");

    System.out.println(expr);

    TajoOptimizer optimizer = new TajoOptimizer(catalog);

    LogicalPlan plan = optimizer.optimize(expr);

    System.out.println(plan.toString());
  }

  @Test
  public void testTPCHQ5Join() throws SQLSyntaxError, VerifyException {
    SQLAnalyzer sqlAnalyzer = new SQLAnalyzer();
    Expr expr = sqlAnalyzer.parse(
        "select n_name from customer, orders, lineitem, supplier, nation, region " +
        "where c_custkey = o_custkey and l_orderkey = o_orderkey and l_suppkey = s_suppkey " +
            "and c_nationkey = s_nationkey and s_nationkey = n_nationkey " +
            "and n_regionkey = r_regionkey and r_name = 'ASIA' " +
            "and o_orderdate >= '1994-01-01' and o_orderdate < '1995-01-01'");

    System.out.println(expr);

    TajoOptimizer optimizer = new TajoOptimizer(catalog);

    LogicalPlan plan = optimizer.optimize(expr);

    System.out.println(plan.toString());
  }

  @Test
  public void testTPCH7Join() throws SQLSyntaxError, VerifyException {
    SQLAnalyzer sqlAnalyzer = new SQLAnalyzer();
    Expr expr = sqlAnalyzer.parse(
        "select supp_nation, cust_nation, l_year from (" +
            "select n1.n_name as supp_nation, n2.n_name as cust_nation " +
            "from supplier,lineitem,orders,customer,nation n1,nation n2 " +
            "where s_suppkey = l_suppkey and o_orderkey = l_orderkey and c_custkey = o_custkey " +
            "and s_nationkey = n1.n_nationkey and c_nationkey = n2.n_nationkey " +
            "and ( (n1.n_name = 'FRANCE' and n2.n_name = 'GERMANY') " +
              "or (n1.n_name = 'GERMANY' and n2.n_name = 'FRANCE')) " +
            "and l_shipdate > '1995-01-01' and l_shipdate < '1996-12-31') as shipping");

    System.out.println(expr);

    TajoOptimizer optimizer = new TajoOptimizer(catalog);

    LogicalPlan plan = optimizer.optimize(expr);

    System.out.println(plan.toString());
  }
}
