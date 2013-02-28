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


import tajo.engine.planner.logical.LogicalNode;

import java.util.Map;

public class LogicalPlan {
  Map<String, Integer> blockGraph;
  Map<Integer, LogicalNode> graph;

  public void addQueryBlock(String blockName, Integer rootId) {
    blockGraph.put(blockName, rootId);
  }

  public Integer getQueryBlockRootId(String blockName) {
    return blockGraph.get(blockName);
  }

  public LogicalNode getQueryBlockPlan(String blockName) {
    return graph.get(blockGraph.get(blockName));
  }
}

