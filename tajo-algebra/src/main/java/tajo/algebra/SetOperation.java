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


import com.google.common.base.Preconditions;

public class SetOperation extends BinaryOperator {
  private boolean distinct = true;

  public SetOperation(ExprType type, Expr left, Expr right, boolean distinct) {
    super(type, left, right);
    Preconditions.checkArgument(type == ExprType.Union ||
        type == ExprType.Intersect ||
        type == ExprType.Except);
  }

  public boolean isDistinct() {
    return distinct;
  }

  public void unsetDistinct() {
    distinct = false;
  }
}
