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
        "part", "supplier", "partsupp", "nation", "region", "lineitem"
    };
    int [] tableVolumns = {
        100, 200, 50, 5, 5, 800
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

    optimizer.transform(expr);
  }

  @Test
  public void testSubQuery() throws SQLSyntaxError, OptimizationException {
    SQLAnalyzer sqlAnalyzer = new SQLAnalyzer();
    Expr expr = sqlAnalyzer.parse(
        "select l_orderkey from lineitem join (select p_partkey from part where p_partkey > 5000) as rtable");

    System.out.println(expr);

    TajoOptimizer optimizer = new TajoOptimizer(catalog);

    optimizer.transform(expr);
  }
}
