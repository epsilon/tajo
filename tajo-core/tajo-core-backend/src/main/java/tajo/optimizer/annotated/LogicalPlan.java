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


import com.google.gson.annotations.Expose;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.optimizer.OptimizationException;
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
  @Expose int rootId;
  @Expose Map<String, Integer> queryBlocks = new HashMap<String, Integer>();
  @Expose Map<Integer, LogicalOp> nodes = new HashMap<Integer, LogicalOp>();
  @Expose Map<String, Integer> relationsPerQueryBlock = new HashMap<String, Integer>();

  /**
   * Each edge indicates a connection from a parent to children.
   * Unary operators (e.g., selection, projection, and so on) have one child.
   * Some binary operators (e.g., join, union and except) have two children.
   */
  @Expose Map<Integer, List<Integer>> inEdges = new HashMap<Integer, List<Integer>>();

  /**
   * Each edge indicates a connection from a child to a parent.
   * In logical algebra, each child has only one parent.
   */
  @Expose Map<Integer, Integer> outEdges = new HashMap<Integer, Integer>();

  private volatile int id;
  private static final Class [] defaultParams = new Class[] {Integer.class};

  public LogicalPlan() {
  }

  public void setRoot(LogicalOp root) {
    checkNode(root);
    rootId = root.getId();
  }

  /**
   * Before adding to a logical plan, every operator should be a valid form.
   * The valid form means that an unary operator has to have a child operator and
   * a binary node has to have both left and right operators.
   *
   * @param op
   */
  private static void validate(LogicalOp op) {
    if (op instanceof UnaryOp) {
      UnaryOp unaryOp = (UnaryOp) op;
      if (unaryOp.getChildOp() == null) {
        throw new OptimizationException(unaryOp.getType()
            + " has to have a child operator before added");
      }
    } else if (op instanceof BinaryOp) {
      BinaryOp binaryOp = (BinaryOp) op;
      if (binaryOp.getLeftOp() == null) {
        throw new OptimizationException(binaryOp.getType()
            + " has to have an outer operator before added");
      }

      if (binaryOp.getRightOp() == null) {
        throw new OptimizationException(binaryOp.getType()
            + " has to have an inner operator before added");
      }
    }
  }

  public void add(LogicalOp node) {
    validate(node);
    nodes.put(node.getId(), node);

    // if an added operator is a relation, add it to relation set.
    switch (node.getType()) {
      case Relation:
        RelationOp relationOp = (RelationOp) node;
        if (relationOp.hasAlias()) {
          relationsPerQueryBlock.put(relationOp.getAlias(), relationOp.getId());
        } else {
          relationsPerQueryBlock.put(relationOp.getName(), relationOp.getId());
        }
        break;

      case TableSubQuery:
        TableSubQueryOp tableSubQuery = (TableSubQueryOp) node;
        relationsPerQueryBlock.put(tableSubQuery.getRelationId(), tableSubQuery.getId());
        queryBlocks.put(tableSubQuery.getName(), tableSubQuery.getId());
        break;
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

  public <T extends LogicalOp> T createOperator(Class<T> clazz) {
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
    RelationOp relationOp = (RelationOp) nodes.get(relationsPerQueryBlock.get(relName));
    Schema schema = relationOp.getSchema();

    String qualifiedName = relationOp.getName() + "." + name;
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
    for (Integer relId : relationsPerQueryBlock.values()) {
      RelationOp rel = (RelationOp) nodes.get(relId);

      String qualifiedName = rel.getName() + "." + name;
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
        width = 2;
        idToWidth.put(node.getId(), width);
      } else {
        int parentId = node.getParentOp().getId();
        if (idToWidth.containsKey(parentId)) {
          width = idToWidth.get(parentId);
          width+=4;
          idToWidth.put(node.getId(), width);
        }
      }

      sb.append(indent(width, node.getPlanString()) + "\n");
    }

    private String indent(int width, String [] planString) {
      String firstLineIndent = new String(new char[width-2]).replace('\0', ' ');
      String indent = new String(new char[width]).replace('\0', ' ');

      StringBuilder sb = new StringBuilder();
      sb.append(firstLineIndent).append(" -> ").append(planString[0]);

      if (planString.length > 1) {
        for (int i = 1; i < planString.length; i++) {
          sb.append("\n   ").append(indent).append(planString[i]);
        }
      }
      return sb.toString();
    }

    public String getOutput() {
      return sb.toString();
    }
  }
}

