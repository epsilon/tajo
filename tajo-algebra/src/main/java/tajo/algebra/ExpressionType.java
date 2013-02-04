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

public enum ExpressionType {
  AND("and"),
  OR("or"),

  Equals("="),
  NotEquals("<>"),
  LessThan("<"),
  LessThanOrEquals("<="),
  GreaterThan(">"),
  GreaterThanOrEquals(">="),

  PLUS("+"),
  MINUS("-"),
  MULTIPLY("*"),
  DIVIDE("/"),
  MOD("%"),

  ColumnRef("column"),
  Function("function"),
  CaseWhen("case_when"),
  Like("like"),
  Literal("literal");

  private String exprName;
  ExpressionType(String name) {
    this.exprName = name;
  }

  public String toString() {
    return exprName;
  }

  public static class JsonSerDer implements JsonSerializer<ExpressionType>,
      JsonDeserializer<ExpressionType> {

    @Override
    public JsonElement serialize(ExpressionType src, Type typeOfSrc,
                                 JsonSerializationContext context) {
      return new JsonPrimitive(src.exprName);
    }

    @Override
    public ExpressionType deserialize(JsonElement json, Type typeOfT,
                                    JsonDeserializationContext context) throws JsonParseException {
      return stringToOperator(json.getAsString());
    }
  }

  public static ExpressionType stringToOperator(String str) {
    ExpressionType operator = null;
    switch (str) {
      case "=": operator = Equals; break;
      case "<>": operator = NotEquals; break;
      case "<": operator = LessThan; break;
      case "<=": operator = LessThanOrEquals; break;
      case ">": operator = GreaterThan; break;
      case ">=": operator = GreaterThanOrEquals; break;

      case "+": operator = PLUS; break;
      case "-": operator = MINUS; break;
      case "*": operator = MULTIPLY; break;
      case "/": operator = DIVIDE; break;
      case "%": operator = MOD; break;

      case "column": operator = ColumnRef; break;
      case "literal": operator = Literal; break;

      default:
        new JsonParseException("Cannot deserialize: " + str);
    }

    return operator;
  }
}
