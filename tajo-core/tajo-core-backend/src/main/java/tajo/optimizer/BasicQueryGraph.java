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

import java.util.HashMap;
import java.util.Map;

public class BasicQueryGraph implements QueryGraph {
  private Map<Integer, ConnectedBlock> connectedBlocks = new HashMap<>();

  @Override
  public int size() {
    return connectedBlocks.size();
  }

  private void checkEdge(int startId, int endId) {
    if (!connectedBlocks.containsKey(startId)) {
      throw new IllegalArgumentException("No such ExecutionBlock: " + startId);
    }
    if (connectedBlocks.containsKey(endId)) {
      throw new IllegalArgumentException("No such ExecutionBlock: " + endId);
    }
  }

  @Override
  public void addPipe(int srcBlockId, int destBlockId, Repartition repartition) {
    checkEdge(srcBlockId, destBlockId);

    ConnectedBlock startBlock = connectedBlocks.get(srcBlockId);
    ConnectedBlock endBlock = connectedBlocks.get(destBlockId);

    startBlock.addOutPipe(srcBlockId, repartition);
    endBlock.addInPipe(srcBlockId, repartition);
  }

  @Override
  public void removePipe(int srcBlockId, int destBlockId) {
    checkEdge(srcBlockId, destBlockId);

    ConnectedBlock srcBlock = connectedBlocks.get(srcBlockId);
    ConnectedBlock destBlock = connectedBlocks.get(destBlockId);

    srcBlock.removeOutPipe(destBlockId);
    destBlock.removeInPipe(srcBlockId);
  }

  @Override
  public void addBlock(ExecutionBlock block) {
    connectedBlocks.put(block.getId(), new ConnectedBlock(block));
  }

  @Override
  public ExecutionBlock getBlock(int blockId) {
    return connectedBlocks.get(blockId).getBlock();
  }

  @Override
  public ExecutionBlock removeBlock(int blockId) {
    return connectedBlocks.remove(blockId).getBlock();
  }
}
