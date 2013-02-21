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

import java.util.Map;

public class ConnectedBlock {
  protected ExecutionBlock block;
  protected Map<Integer, Repartition> inEdges;
  protected Map<Integer, Repartition> outEdges;

  ConnectedBlock(ExecutionBlock block) {
    this.block = block;
  }

  public int getId() {
    return block.getId();
  }

  public ExecutionBlock getBlock() {
    return this.block;
  }

  public void addInPipe(int srcBlockId, Repartition repartition) {
    inEdges.put(srcBlockId, repartition);
  }

  public Repartition removeInPipe(int srcBlockId) {
    return inEdges.remove(srcBlockId);
  }

  public void addOutPipe(int destBlockId, Repartition repartition) {
    outEdges.put(destBlockId, repartition);
  }

  public Repartition removeOutPipe(int destBlockId) {
    return outEdges.remove(destBlockId);
  }

  public Repartition getInPipe(int id) {
    return inEdges.get(id);
  }

  public Repartition getOutPipe(int id) {
    return outEdges.get(id);
  }

  public Iterable<Repartition> getInEdges() {
    return inEdges.values();
  }

  public Iterable<Repartition> getOutEdges() {
    return outEdges.values();
  }
}