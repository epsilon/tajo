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


import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.optimizer.VerifyException;
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
  Map<String, Integer> relations = new HashMap<String, Integer>();

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

    if (node instanceof UnaryOp) {
      UnaryOp unaryOp = (UnaryOp) node;
      connect(unaryOp.child.getId(), unaryOp.getId());
    } else if (node instanceof BinaryOp) {
      BinaryOp binaryOp = (BinaryOp) node;
      connect(binaryOp.getOuterNode().getId(), binaryOp.getId());
      connect(binaryOp.getInnerNode().getId(), binaryOp.getId());
    } else {
      switch (node.getType()) {
        case Relation:
          RelationOp relationOp = (RelationOp) node;
          if (relationOp.hasAlias()) {
            relations.put(relationOp.getAlias(), relationOp.getId());
          } else {
            relations.put(relationOp.getTableId(), relationOp.getId());
          }
          break;
        case RelationList:
          RelationListOp relList = (RelationListOp) node;
          for (LogicalOp rel : relList.relations) {
            connect(rel.getId(), relList.getId());
          }
        case ScalarSubQuery:
          break;
        case TableSubQuery:
          TableSubQueryOp tableSubQuery = (TableSubQueryOp) node;
          blockGraph.put(tableSubQuery.getName(), tableSubQuery.getId());
          break;
      }
    }
  }

  public void connect(int child, int parent) {
    checkEdge(child, parent);

    if (inEdges.containsKey(parent)) {
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

  public LogicalOp getRootOperator() {
    return nodes.get(rootId);
  }

  public LogicalOp getOperator(int id) {
    return nodes.get(id);
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

  public Column getColumn(String relName, String name) throws VerifyException {
    RelationOp relationOp = (RelationOp) nodes.get(relations.get(relName));
    Schema schema = relationOp.getSchema();

    String qualifiedName = relationOp.getTableId() + "." + name;
    if (!schema.contains(qualifiedName)) {
      throw new VerifyException("ERROR: no such a column "+ name);
    }

    Column column;
    try {
      column = (Column) schema.getColumn(qualifiedName).clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    if (relationOp.hasAlias()) {
      column.setName(relName + "." + column.getColumnName());
    }
    return column;
  }

  public Column getColumn(String name) throws VerifyException {
    Column candidate = null;
    int cnt = 0;
    for (Integer relId : relations.values()) {
      RelationOp rel = (RelationOp) nodes.get(relId);

      String qualifiedName = rel.getTableId() + "." + name;
      if (rel.getSchema().contains(qualifiedName)) {
        if (cnt > 0) {
          throw new VerifyException("ERROR: column name "+ name + " is ambiguous");
        }
        cnt++;
        try {
          candidate = (Column) rel.getSchema().getColumn(qualifiedName).clone();
        } catch (CloneNotSupportedException e) {
          throw new RuntimeException(e);
        }
        if (rel.hasAlias()) {
          candidate.setName(rel.getAlias() + "." + name);
        }
      }
    }
    if (candidate == null) {
      throw new VerifyException("ERROR: no such a column "+ name);
    }

    return candidate;
  }

  public String toString() {
    Visitor vis = new Visitor();
    getRootOperator().preOrder(vis);
    return vis.getOutput();
  }

  public LogicalOp getParent(int id) {
    return nodes.get(outEdges.get(id));
  }

  private class Visitor implements LogicalOpVisitor {
    Map<Integer, Integer> idToWidth = new HashMap<Integer, Integer>();
    int width;
    StringBuilder sb = new StringBuilder();

    @Override
    public void visit(LogicalOp node) {
      if (node.getId() == rootId) {
        width = 0;
        idToWidth.put(node.getId(), width);
      } else {
        int parentId = getParent(node.getId()).getId();
        if (idToWidth.containsKey(parentId)) {
          width = idToWidth.get(parentId);
          width+=2;
          idToWidth.put(node.getId(), width);
        }
      }

      sb.append(indent(width) + " " + node.getType() + "\n");
    }

    private String indent(int width) {
      StringBuilder indent = new StringBuilder();
      for (int i = 0; i < width; i++) {
        indent.append(" ");
      }
      return indent.toString();
    }

    public String getOutput() {
      return sb.toString();
    }
  }
}

