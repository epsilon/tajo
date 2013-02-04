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
import com.google.gson.GsonBuilder;

public class JsonHelper {
  private static JsonHelper instance;

  private GsonBuilder builder;
  private Gson gson;

  static {
    instance = new JsonHelper();
  }

  private JsonHelper() {
    initBuilder();
    gson = builder.create();
  }

  private void initBuilder() {
    builder = new GsonBuilder().setPrettyPrinting();
    builder.registerTypeAdapter(ExpressionType.class, new ExpressionType.JsonSerDer());
    builder.registerTypeAdapter(RelationalOp.class, new RelationalOp.AlgebraJsonSerDer());
  }


  public static Gson getInstance() {
    return instance.gson;
  }

  public static String toJson(Object obj) {
    return instance.gson.toJson(obj);
  }

  public static Object fromJson(String json, Class clazz) {
    return instance.gson.fromJson(json, clazz);
  }
}
