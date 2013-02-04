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

import static tajo.algebra.SortOp.SortSpec;

public class TestSelectionExpr {

  @Test
  public void test() {
    Relation rel = new Relation("employee");
    SelectionOp sel = new SelectionOp(rel);
    System.out.println(sel.toString());
  }

  @Test
  public void test2() {
    Relation rel = new Relation("employee");
    SelectionOp sel = new SelectionOp(rel);

    SelectionOp sel2 = new SelectionOp(new ReferRelation("refer1", sel));
    System.out.println(sel2.toString());
  }

  @Test
  public void testSort() {
    Relation rel = new Relation("employee");
    SelectionOp sel = new SelectionOp(rel);

    SelectionOp sel2 = new SelectionOp(new ReferRelation("refer1", sel));

    System.out.println(sel2.toJson());

    SortOp sort = new SortOp();
    SortSpec spec = new SortSpec("employeeId", true, true);
    sort.setSortSpecs(new SortSpec[] {spec});
    sort.setSubOp(sel2);

    String json = sort.toJson();
    System.out.println(json);
    //RelationalOp restored = (RelationalOp) JsonHelper.fromJson(json, RelationalOp.class);
  }
}
