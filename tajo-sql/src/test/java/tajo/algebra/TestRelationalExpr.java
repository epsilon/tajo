package tajo.algebra;

import org.junit.Test;
import tajo.TajoTestingCluster;
import tajo.benchmark.TPCH;
import tajo.catalog.CatalogService;
import tajo.catalog.FunctionDesc;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.LogicalPlanner;
import tajo.master.TajoMaster;

public class TestRelationalExpr {
  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static QueryAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static TPCH tpch;

  @Test
  public void test() throws Exception {
    util = new TajoTestingCluster();
    util.startCatalogCluster();
    catalog = util.getMiniCatalogCluster().getCatalog();
    for (FunctionDesc funcDesc : TajoMaster.initBuiltinFunctions()) {
      catalog.registerFunction(funcDesc);
    }


  }
}
