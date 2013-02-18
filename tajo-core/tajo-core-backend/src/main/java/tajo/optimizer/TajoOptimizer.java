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

import tajo.algebra.*;
import tajo.catalog.CatalogService;
import tajo.catalog.TableDesc;
import tajo.engine.planner.logical.ExprType;
import tajo.engine.planner.logical.*;
import tajo.util.TUtil;

import java.util.*;

public class TajoOptimizer extends AbstractOptimizer {
  private CatalogService catalog;

  public TajoOptimizer(CatalogService catalog) {
    this.catalog = catalog;
  }

  @Override
  public LogicalNode optimize(Expr algebra) throws OptimizationException {

    verify(algebra);

    LogicalNode [] joinEnumerated = enumeareJoinOrder();
    PriorityQueue<CostedPlan> heap = new PriorityQueue();

    for (LogicalNode joinOrder : joinEnumerated) {
      LogicalNode step1 = pushdownSelection(joinOrder);
      LogicalNode step2 = pushdownProjection(step1);
      CostedPlan costedPlan = computeCost(step2);
      heap.add(costedPlan);
    }

    return heap.poll().plan;
  }

  public LogicalNode transform(Expr expr) throws OptimizationException {
    LogicalNode child;
    LogicalNode left;
    LogicalNode right;
    switch (expr.getType()) {
      case Projection:
        Projection projection = (Projection) expr;
        child = transform(projection.getChild());
        ProjectionNode projNode = new ProjectionNode(null);
        break;

      case RelationList:
        RelationList relationList = (RelationList) expr;
        createImplicitJoinPlan(relationList);
        break;

      case Join:
        Join join = (Join) expr;
        break;

      case Relation:
        Relation relation = (Relation) expr;
        verifyRelation(relation);
        TableDesc desc = catalog.getTableDesc(relation.getName());
        ScanNode scanNode = new ScanNode(relation, desc.getMeta().getSchema());
        return scanNode;
    }

    return null;
  }

  private LogicalNode createImplicitJoinPlan(RelationList expr) throws OptimizationException {
    Set<Expr> relSet = TUtil.newHashSet(expr.getRelations());
    Set<Expr> nextRemain;
    Set<JoinNode> enumerated = new HashSet<>();
    for (Expr rel : relSet) {
      nextRemain = new HashSet<>(relSet);
      nextRemain.remove(rel);
      enumerated.addAll(enumerateJoin(rel, nextRemain));
    }

    int i = 0;
    for (JoinNode joinNode : enumerated) {
      System.out.println((i++) + " : " + printJoinOrder(joinNode));
    }

    return null;
  }

  private String printJoinOrder(JoinNode joinNode) {
   List<String> relList = new ArrayList<>();
    traverseJoinNode(joinNode, relList);
    return relList.toString();
  }

  private void traverseJoinNode(LogicalNode node, List<String> result) {
    if (node.getType() == ExprType.JOIN) {
      JoinNode join = (JoinNode) node;
      traverseJoinNode(join.getOuterNode(), result);
      traverseJoinNode(join.getInnerNode(), result);
    } else if (node.getType() == ExprType.SCAN) {
      ScanNode scan = (ScanNode) node;
      result.add(scan.getTableId());
    }
  }

  private List<JoinNode> enumerateJoin(Expr rel, Set<Expr> remain) throws OptimizationException {
    List<JoinNode> enumerated = new ArrayList<>();
    if (remain.size() == 1) {
      enumerated.add(new JoinNode(JoinType.CROSS_JOIN,
          transform(rel), transform(remain.iterator().next())));
    } else {
      Set<Expr> nextRemain;
      for (Expr next : remain) {
        nextRemain = new HashSet<>(remain);
        nextRemain.remove(next);
        enumerated.addAll(createJoinNode(rel, enumerateJoin(next, nextRemain)));
        enumerated.addAll(createJoinNode(enumerateJoin(next, nextRemain), rel));
      }
    }

    return enumerated;
  }

  private List<JoinNode> createJoinNode(Expr outer, List<JoinNode> enumerated) throws OptimizationException {
    List<JoinNode> joins = new ArrayList<>();
    for (JoinNode join : enumerated) {
      joins.add(new JoinNode(JoinType.CROSS_JOIN, transform(outer), join));
    }
    return joins;
  }

  private List<JoinNode> createJoinNode(List<JoinNode> enumerated, Expr inner) throws OptimizationException {
    List<JoinNode> joins = new ArrayList<>();
    for (JoinNode join : enumerated) {
      joins.add(new JoinNode(JoinType.CROSS_JOIN, join, transform(inner)));
    }
    return joins;
  }

  private LogicalNode createExplicitJoinPlan(Join join) {
    return null;
  }

  public CostedPlan computeCost(LogicalNode optimized) {
    return null;
  }

  public class CostedPlan implements Comparable<CostedPlan> {
    double cost;
    LogicalNode plan;

    public CostedPlan(LogicalNode plan) {
      this.plan = plan;
    }

    public void setCost(double cost) {
      this.cost = cost;
    }

    public double getCost() {
      return this.cost;
    }

    @Override
    public int compareTo(CostedPlan o) {
      double compval = cost - o.cost;
      if (compval < 0) {
        return -1;
      } else if (compval > 0) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  private LogicalNode pushdownProjection(LogicalNode plan) {
    return null;
  }

  private LogicalNode pushdownSelection(LogicalNode plan) {
    return null;
  }

  @Override
  void verify(Expr algebra) throws OptimizationException {
  }

  private void verifyRelation(Relation relation) throws VerifyException {
    if (!catalog.existsTable(relation.getName())) {
      throw new VerifyException("No such relation: " + relation.getName());
    }
  }

  LogicalNode[] enumeareJoinOrder() {
    return new LogicalNode[0];  //To change body of implemented methods use File | Settings | File Templates.
  }
}
