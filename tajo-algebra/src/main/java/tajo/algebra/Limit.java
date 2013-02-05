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


public class Limit extends UnaryOperator {
  private long fetch_first_num;

  public Limit(long fetch_first_num) {
    this.fetch_first_num = fetch_first_num;
  }

  public long getLimitRow() {
    return this.fetch_first_num;
  }

  @Override
  public String toString() {
    return toJson();
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  public boolean equals(Object obj) {
    return obj instanceof Limit &&
        fetch_first_num == ((Limit)obj).fetch_first_num;
  }
}
