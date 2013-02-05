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

import com.google.gson.*;

import java.lang.reflect.Type;

public enum ExprType {
  Relation(tajo.algebra.Relation.class),
  Except,
	Aggregation,
  Intersect,
  Join(tajo.algebra.Join.class),
  Projection(tajo.algebra.Projection.class),
  Rename,
  Selection(tajo.algebra.Selection.class),
  Sort(tajo.algebra.Sort.class),
  Union,
  Limit(tajo.algebra.Limit.class),

  And(BinaryExpr.class),
  Or(BinaryExpr.class),

  Equals(BinaryExpr.class),
  NotEquals(BinaryExpr.class),
  LessThan(BinaryExpr.class),
  LessThanOrEquals(BinaryExpr.class),
  GreaterThan(BinaryExpr.class),
  GreaterThanOrEquals(BinaryExpr.class),

  Plus(BinaryExpr.class),
  Minus(BinaryExpr.class),
  Multiply(BinaryExpr.class),
  Divide(BinaryExpr.class),
  Mod(BinaryExpr.class),

  ColumnRef(ColumnRefExpr.class),
  Function(FunctionExpr.class),
  CaseWhen(CaseWhenExpr.class),
  Like(LikeExpr.class),
  Literal(LiteralExpr.class);

  private Class baseClass;

  ExprType() {
    this.baseClass = Expr.class;
  }
  ExprType(Class clazz) {
    this.baseClass = clazz;
  }

  public Class getBaseClass() {
    return this.baseClass;
  }

  public static class JsonSerDer implements JsonSerializer<ExprType>,
                                            JsonDeserializer<ExprType> {

    @Override
    public JsonElement serialize(ExprType src, Type typeOfSrc,
                                 JsonSerializationContext context) {
      return new JsonPrimitive(src.name());
    }

    @Override
    public ExprType deserialize(JsonElement json, Type typeOfT,
                                      JsonDeserializationContext context)
        throws JsonParseException {
      return ExprType.valueOf(json.getAsString());
    }
  }
}