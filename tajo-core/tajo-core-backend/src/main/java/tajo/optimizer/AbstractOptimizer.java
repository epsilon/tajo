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

package tajo.optimizer;

import tajo.algebra.Expr;
import tajo.engine.planner.logical.LogicalNode;

public abstract class AbstractOptimizer {
  public abstract LogicalNode optimize(Expr algebra) throws OptimizationException;

  /**
   * Should check the following:
   * <ul>
   *   <li>relations exists in the catalog</li>
   *   <li>columns exist in corresponding relations</li>
   *   <li>operands and operators are used correctly/li>
   * </ul>
   *
   * @param algebra to be verified
   * @throws OptimizationException
   */
  abstract void verify(Expr algebra) throws OptimizationException;
}
