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

import static tajo.algebra.Sort.SortSpec;

public class TestSelectionExpr {

  @Test
  public void test() {
    Relation rel = new Relation("employee");
    Selection sel = new Selection(rel);
    System.out.println(sel.toString());
  }

  @Test
  public void test2() {
    Relation rel = new Relation("employee");
    Selection sel = new Selection(rel);

    Selection sel2 = new Selection(new ReferRelation("refer1", sel));
    System.out.println(sel2.toString());
  }

  @Test
  public void testSort() {
    Relation rel = new Relation("employee");
    Selection sel = new Selection(rel);

    Selection sel2 = new Selection(new ReferRelation("refer1", sel));

    System.out.println(sel2.toJson());


    SortSpec spec = new SortSpec(new ColumnRefExpr("employeeId"), true, true);
    Sort sort = new Sort(new SortSpec[]{spec});
    sort.setChild(sel2);

    String json = sort.toJson();
    System.out.println(json);
    //Expr restored = (Expr) JsonHelper.fromJson(json, Expr.class);
  }
}
