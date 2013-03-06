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


import tajo.algebra.ColumnReferenceExpr;
import tajo.algebra.Projection;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.optimizer.OptimizationException;
import tajo.optimizer.TajoOptimizer;
import tajo.optimizer.VerifyException;

import java.lang.reflect.Constructor;
import java.util.*;

/**
 * not thread safe
 */
public class LogicalPlan {
  private int rootId;
  private Map<Integer, LogicalOp> nodes = new HashMap<Integer, LogicalOp>();
  private Map<String, Integer> queryBlocks = new HashMap<String, Integer>();
  private Map<String, Map<String, Integer>> relationsPerQueryBlock =
      new HashMap<String, Map<String, Integer>>();
  private Map<String, Projection> projectionPerQueryBlock =
      new HashMap<String, Projection>();
  private Map<String, Schema> schemaPerQueryBlock =
      new HashMap<String, Schema>();
  private Map<String, ProjectionOp> projectionOpPerQueryBlock =
      new HashMap<String, ProjectionOp>();
  private Set<String> endFromClausePerBlock = new HashSet<String>();

  private volatile int sequenceId;
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

  public void addProjection(String blockId, Projection projection) {
    this.projectionPerQueryBlock.put(blockId, projection);
  }

  private void addRelation(String blockId, String relationName, int id) {
    if (relationsPerQueryBlock.containsKey(blockId)) {
      relationsPerQueryBlock.get(blockId).put(relationName, id);
    } else {
      Map<String, Integer> relations = new HashMap<String, Integer>();
      relations.put(relationName, id);
      relationsPerQueryBlock.put(blockId, relations);
    }
  }

  private boolean checkEndFromClause(String blockId, LogicalOp node) {
    return !(node.getType() == OpType.Relation ||
         node.getType() == OpType.RelationList ||
         node.getType() == OpType.JOIN);
  }

  public void add(String blockId, LogicalOp node) throws VerifyException {
    validate(node);

    if (!endFromClausePerBlock.contains(blockId)) {
      if (checkEndFromClause(blockId, node)) {
        endFromClausePerBlock.add(blockId);
        ProjectionOp projectionOp = TajoOptimizer.createProjectionOp(this, blockId, null);
        projectionOpPerQueryBlock.put(blockId, projectionOp);
      }
    }


    nodes.put(node.getId(), node);

    // if an added operator is a relation, add it to relation set.
    switch (node.getType()) {
      case Relation:
        RelationOp relationOp = (RelationOp) node;
        addRelation(blockId, relationOp.getCanonicalName(), relationOp.getId());
        break;

      case TableSubQuery:
        TableSubQueryOp tableSubQuery = (TableSubQueryOp) node;
        addRelation(blockId, tableSubQuery.getCanonicalName(), tableSubQuery.getId());
        queryBlocks.put(tableSubQuery.getName(), tableSubQuery.getId());
        break;
    }
  }

  public LogicalOp getRootOperator() {
    return nodes.get(rootId);
  }

  public LogicalOp getOperator(int id) {
    return nodes.get(id);
  }

  public LogicalOp getBlockRoot(String blockId) {
    int blockRootId = queryBlocks.get(blockId);
    return nodes.get(blockRootId);
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
      node = (T) cons.newInstance(new Object[] {sequenceId++});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return node;
  }

  public Column findColumn(String blockId, String relName, String name)
      throws VerifyException {

    RelationOp relationOp = (RelationOp)
        nodes.get(relationsPerQueryBlock.get(blockId).get(relName));

    // if a column name is outside of this query block
    if (relationOp == null) {
      // TODO - nested query can only refer outer query block? or not?
      for (Map<String, Integer> entry : relationsPerQueryBlock.values()) {
        if (entry.containsKey(relName)) {
          relationOp = (RelationOp) nodes.get(entry.get(relName));
        }
      }
    }

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

    column.setName(relName + "." + column.getColumnName());

    return column;
  }

  private Column findColumnFromRelationOp(RelationOp relation, String name) {
    String qualifiedName = relation.getName() + "." + name;
    Column candidate;
    if (relation.getSchema().contains(qualifiedName)) {
      try {
        candidate = (Column) relation.getSchema().getColumn(qualifiedName).clone();
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException(e);
      }
      candidate.setName(relation.getCanonicalName() + "." + name);
      return candidate;
    } else {
      return null;
    }
  }

  public Column findColumn(String blockId, ColumnReferenceExpr columnRef) throws VerifyException {
    if (columnRef.hasRelationName()) {
      return findColumn(blockId, columnRef.getRelationName(), columnRef.getName());
    } else {
      return findColumn(blockId, columnRef.getName());
    }
  }

  public Column findColumn(String blockId, String name) throws VerifyException {
    List<Column> candidates = new ArrayList<Column>();
    Column candidate = null;

    // Try to find a column from the current query block
    for (Integer relId : relationsPerQueryBlock.get(blockId).values()) {
      RelationOp rel = (RelationOp) nodes.get(relId);
      candidate = findColumnFromRelationOp(rel, name);
      if (candidate != null) {
        candidates.add(candidate);
        if (candidates.size() > 1) {
          break;
        }
      }
    }

    // if a column is not found, try to find the column from outer blocks.
    if (candidates.isEmpty()) {
      // for each block
      Outer: for (Map<String, Integer> entry : relationsPerQueryBlock.values()) {
        for (Integer relId : entry.values()) {
          RelationOp rel = (RelationOp) nodes.get(relId);
          candidate = findColumnFromRelationOp(rel, name);
          if (candidate != null) {
            candidates.add(candidate);
            if (candidates.size() > 1)
              break Outer;
          }
        }
      }
    }

    if (candidates.isEmpty()) {
      throw new VerifyException("ERROR: no such a column "+ name);
    } else  if (candidates.size() > 1) {
      throw new VerifyException("ERROR: column name "+ name + " is ambiguous");
    }

    return candidates.get(0);
  }

  public String toString() {
    Visitor vis = new Visitor();
    getRootOperator().preOrder(vis);
    return vis.getOutput();
  }

  private class Visitor implements LogicalOpVisitor {
    Map<Integer, Integer> idToWidth = new HashMap<Integer, Integer>();
    int width;
    StringBuilder sb = new StringBuilder();

    @Override
    public boolean accept(LogicalOp node) {
      return true;
    }

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

    private String indent(int width, List<String> planStrings) {
      String firstLineIndent = new String(new char[width-2]).replace('\0', ' ');
      String indent = new String(new char[width]).replace('\0', ' ');

      StringBuilder sb = new StringBuilder();
      sb.append(firstLineIndent).append(" -> ").append(planStrings.get(0));

      if (planStrings.size() > 1) {
        for (int i = 1; i < planStrings.size(); i++) {
          sb.append("\n   ").append(indent).append(planStrings.get(i));
        }
      }
      return sb.toString();
    }

    public String getOutput() {
      return sb.toString();
    }
  }
}

