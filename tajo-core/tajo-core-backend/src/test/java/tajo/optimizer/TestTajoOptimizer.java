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
import org.junit.Before;
import tajo.TajoTestingCluster;
import tajo.benchmark.TPCH;
import tajo.catalog.*;
import tajo.catalog.proto.CatalogProtos;
import tajo.master.TajoMaster;

public class TestTajoOptimizer {
  private TajoTestingCluster util;
  private TPCH tpch;
  private CatalogService catalog;

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
        "part", "supplier", "partsupp", "nation", "region"
    };
    tpch = new TPCH();
    tpch.loadSchemas();
    tpch.loadOutSchema();
    for (String table : tpchTables) {
      TableMeta m = TCatUtil.newTableMeta(tpch.getSchema(table), CatalogProtos.StoreType.CSV);
      TableDesc d = TCatUtil.newTableDesc(table, m, new Path("file:///"));
      catalog.addTable(d);
    }
  }

  public void testJoinEnumeration() {
     TajoOptimizer optimizer = new TajoOptimizer();

  }
}
