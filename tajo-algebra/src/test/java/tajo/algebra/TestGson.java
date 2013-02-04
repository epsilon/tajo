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

import static junit.framework.Assert.assertEquals;

public class TestGson {

  @Test
  public void test1() {
    Gson gson = new Gson();
    System.out.println(gson.toJson(1));
    System.out.println(gson.toJson("abcd"));
    System.out.println(gson.toJson(new Long(10)));
    int [] values = {1,2};
    System.out.println(gson.toJson(values));
  }

  class BagOfPrimitives {
    private int value1 = 1;
    private String value2 = "abc";
    private int value3 = 3;
    BagOfPrimitives() {
      // no-args constructor
    }

    @Override
    public boolean equals(Object obj) {
      BagOfPrimitives primitive = (BagOfPrimitives) obj;
      boolean test1 = value1 == primitive.value1;
      boolean test2 = value2.equals(primitive.value2);
      boolean test3 = value3 == primitive.value3;

      return test1 && test2 && test3;
    }
  }

  @Test
  public void test2() {
    BagOfPrimitives obj = new BagOfPrimitives();
    Gson gson = new Gson();
    String json = gson.toJson(obj);

    BagOfPrimitives obj2 = gson.fromJson(json, BagOfPrimitives.class);
    assertEquals(obj, obj2);
  }
}
