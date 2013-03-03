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


import tajo.util.TUtil;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * not thread safe
 */
public class LogicalPlan {
  int rootId;
  Map<String, Integer> blockGraph = new HashMap<String, Integer>();
  Map<Integer, LogicalOp> nodes = new HashMap<Integer, LogicalOp>();

  /**
   * Each edge indicates a connection from a parent to children.
   * Unary operators (e.g., selection, projection, and so on) have one child.
   * Some binary operators (e.g., join, union and except) have two children.
   */
  Map<Integer, List<Integer>> inEdges = new HashMap<Integer, List<Integer>>();

  /**
   * Each edge indicates a connection from a child to a parent.
   * In logical algebra, each child has only one parent.
   */
  Map<Integer, Integer> outEdges = new HashMap<Integer, Integer>();

  private volatile int id;
  private static final Class [] defaultParams = new Class[] {Integer.class};

  public LogicalPlan() {
  }

  public void setRoot(LogicalOp root) {
    checkNode(root);
    rootId = root.getId();
  }

  public void add(LogicalOp node) {
    nodes.put(node.getId(), node);
    switch (node.getType()) {
      case JOIN:
        JoinOp join = (JoinOp) node;
        connect(join.getOuterNode().getId(), join.getId());
        connect(join.getInnerNode().getId(), join.getId());
        break;
      case ScalarSubQuery:

      case TableSubQuery:
        TableSubQueryOp tableSubQuery = (TableSubQueryOp) node;
        blockGraph.put(tableSubQuery.getName(), tableSubQuery.getId());
        break;
    }
  }

  public void connect(int child, int parent) {
    checkEdge(child, parent);

    if (inEdges.containsKey(child)) {
      inEdges.get(parent).add(child);
    } else {
      inEdges.put(parent, TUtil.newList(child));
    }

    if (outEdges.containsKey(child)) {
      throw new IllegalArgumentException("This node (" + child
          + ") already have a parent (" + parent + ")");
    }
    outEdges.put(child, parent);
  }

  private void checkNode(LogicalOp op) {
    if (!nodes.containsKey(op.getId())) {
      throw new IllegalArgumentException("No such a node corresponding to id (" + op.getId() +")");
    }
  }

  private void checkEdge(int src, int dest) {
    if (!nodes.containsKey(src)) {
      throw new IllegalArgumentException("No such a src node corresponding to id (" + src +")");
    }

    if (!nodes.containsKey(dest)) {
      throw new IllegalArgumentException("No such a destination node corresponding to id ("
          + dest +")");
    }
  }

  public <T extends LogicalOp> T createLogicalOp(Class<T> clazz) {
    T node;
    try {
      Constructor cons = clazz.getConstructor(defaultParams);
      node = (T) cons.newInstance(new Object[] {id++});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return node;
  }
}

