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

public abstract class BinaryOp extends RelationalOp {
  RelationalOp outer;
  RelationalOp inner;

  @SuppressWarnings("unused")
  BinaryOp() {}

  BinaryOp(OperatorType opType) {
    super(opType);
  }

  public BinaryOp(OperatorType type, RelationalOp outer, RelationalOp inner) {
    super(type);
    this.outer = outer;
    this.inner = inner;
  }

  public RelationalOp getOuter() {
    return this.outer;
  }

  public void setOuter(RelationalOp outer) {
    this.outer = outer;
  }

  public RelationalOp getInner() {
    return this.outer;
  }

  public void setInner(RelationalOp inner) {
    this.inner = inner;
  }
}
