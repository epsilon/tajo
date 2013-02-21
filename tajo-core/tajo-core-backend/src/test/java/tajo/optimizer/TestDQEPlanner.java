package tajo.optimizer;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import tajo.TajoTestingCluster;
import tajo.benchmark.TPCH;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos;
import tajo.engine.parser.QueryAnalyzer;
import tajo.engine.planner.LogicalPlanner;
import tajo.engine.planner.PlanningContext;
import tajo.engine.planner.logical.LogicalNode;
import tajo.master.TajoMaster;

public class TestDQEPlanner {
  private static TajoTestingCluster util;
  private static CatalogService catalog;
  private static QueryAnalyzer analyzer;
  private static LogicalPlanner planner;
  private static TPCH tpch;

  @Before
  public void setup() throws Exception {
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
    tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadOutSchema();
    for (String table : tpchTables) {
      TableMeta m = TCatUtil.newTableMeta(tpch.getSchema(table), CatalogProtos.StoreType.CSV);
      TableDesc d = TCatUtil.newTableDesc(table, m, new Path("file:///"));
      catalog.addTable(d);
    }

    analyzer = new QueryAnalyzer(catalog);
    planner = new LogicalPlanner(catalog);
  }

  @After
  public void tearDown() {

  }

  private LogicalNode getPlan(String sql) {
    PlanningContext ctx = analyzer.parse(sql);
    return planner.createPlan(ctx);
  }

  @Test
  public void test() {
    LogicalNode plan = getPlan("select count(*) from lineitem");
    System.out.println(plan.toJSON());
  }
}
