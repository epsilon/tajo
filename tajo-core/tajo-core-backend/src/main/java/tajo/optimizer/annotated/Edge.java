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

package tajo.optimizer.annotated;

import tajo.engine.eval.EvalNode;

public class Edge {
  private String src;
  private String target;
  private EvalNode joinQual;

  public Edge(String src, String target, EvalNode joinQual) {
    this.src = src;
    this.target = target;
    this.joinQual = joinQual;
  }

  public String getSrc() {
    return this.src;
  }

  public String getTarget() {
    return this.target;
  }

  public EvalNode getJoinQual() {
    return this.joinQual;
  }

  @Override
  public String toString() {
    return "(" + src + "=> " + target + ", " + joinQual + ")";
  }
}