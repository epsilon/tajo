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

import com.google.gson.Gson;
import org.junit.Test;
import tajo.algebra.LiteralExpr.LiteralType;

public class TestArithmeticExpr {

  @Test
  public void test() {
    Expr expr1 = new BinaryExpr(ExprType.Plus,
        new LiteralExpr("1", LiteralType.Unsigned_Integer),
        new LiteralExpr("2", LiteralType.Unsigned_Integer));
    Expr expr2 = new BinaryExpr(ExprType.Minus,
        new LiteralExpr("1", LiteralType.Unsigned_Integer),
        expr1);

    String json = expr2.toString();

    Gson gson = JsonHelper.getInstance();
    Expr expr3 = gson.fromJson(json, Expr.class);

    System.out.println(expr3.toString());
  }
}
