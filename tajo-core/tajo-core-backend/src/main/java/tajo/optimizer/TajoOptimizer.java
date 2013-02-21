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
//      CostedPlan costedPlan = computeCost(step2);
//      heap.add(costedPlan);
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

      case Selection:
        Selection selection = (Selection) expr;
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

      // the below will has separate query blocks
      case ScalarSubQuery:

      case TableSubQuery:

      case Union:

      case Intersect:

      case Except:
    }

    return null;
  }

  private LogicalNode createImplicitJoinPlan(RelationList expr) throws OptimizationException {
    Set<Expr> relSet = TUtil.newHashSet(expr.getRelations());
    Set<Expr> nextRemain;
    Set<JoinNode> enumerated = new HashSet<>();
    JoinNode bestPlan = null;
    Collection<JoinNode> candidate;
    int i = 1;
    JoinEnumerator enuemrator = new RightDeepEnumerator();
    nextRemain = new HashSet<>(relSet);
    candidate = enuemrator.enumerate(nextRemain);
    for (JoinNode joinNode : candidate) {
      System.out.print((i++) + " : ");
      printJoinOrder(joinNode);
      System.out.print("(cost: " + computeCost(joinNode) + ")");
      System.out.println();
    }
    bestPlan = (JoinNode) findBestPlan(candidate, bestPlan);

    System.out.println("=======================================");
    System.out.print("best plan: ");
    printJoinOrder(bestPlan);
    System.out.print("(cost: " + computeCost(bestPlan) + ")");

    return null;
  }

  private interface JoinEnumerator {
    Collection<JoinNode> enumerate(Set<Expr> exprs) throws OptimizationException;
  }

  @SuppressWarnings("unused")
  private class LeftDeepEnumerator implements JoinEnumerator {

    @Override
    public Collection<JoinNode> enumerate(Set<Expr> exprs) throws OptimizationException {
      Set<JoinNode> enumerated = new HashSet<>();
      Set<Expr> nextRemain;
      for (Expr rel : exprs) {
        nextRemain = new HashSet<>(exprs);
        nextRemain.remove(rel);
        enumerated.addAll(enumerateLeftDeepJoin(nextRemain, rel));
      }

      return enumerated;
    }

    private Collection<JoinNode> enumerateLeftDeepJoin(Set<Expr> remain, Expr rel)
        throws OptimizationException {
      List<JoinNode> enumerated = new ArrayList<>();
      if (remain.size() == 1) {
        enumerated.add(new JoinNode(JoinType.CROSS_JOIN,
            transform(rel), transform(remain.iterator().next())));
      } else {
        Set<Expr> nextRemain;
        for (Expr next : remain) {
          nextRemain = new HashSet<>(remain);
          nextRemain.remove(next);
          enumerated.addAll(createLeftDeepJoin(enumerateLeftDeepJoin(nextRemain, next), rel));
        }
      }

      return enumerated;
    }

    private List<JoinNode> createLeftDeepJoin(Collection<JoinNode> enumerated, Expr inner) throws OptimizationException {
      List<JoinNode> joins = new ArrayList<>();
      for (JoinNode join : enumerated) {
        joins.add(new JoinNode(JoinType.CROSS_JOIN, join, transform(inner)));
      }
      return joins;
    }
  }

  private class RightDeepEnumerator implements JoinEnumerator {

    @Override
    public Collection<JoinNode> enumerate(Set<Expr> exprs) throws OptimizationException {
      Set<JoinNode> enumerated = new HashSet<>();
      Set<Expr> nextRemain;
      for (Expr rel : exprs) {
        nextRemain = new HashSet<>(exprs);
        nextRemain.remove(rel);
        enumerated.addAll(enumerateRightDeepJoin(rel, nextRemain));
      }

      return enumerated;
    }

    private Collection<JoinNode> enumerateRightDeepJoin(Expr rel, Set<Expr> remain)
        throws OptimizationException {
      List<JoinNode> enumerated = new ArrayList<>();
      if (remain.size() == 1) {
        enumerated.add(new JoinNode(JoinType.CROSS_JOIN,
            transform(rel), transform(remain.iterator().next())));
      } else {
        Set<Expr> nextRemain;
        for (Expr next : remain) {
          nextRemain = new HashSet<>(remain);
          nextRemain.remove(next);
          enumerated.addAll(createRightDeepJoin(rel, enumerateRightDeepJoin(next, nextRemain)));
        }
      }

      return enumerated;
    }

    private List<JoinNode> createRightDeepJoin(Expr outer, Collection<JoinNode> enumerated) throws OptimizationException {
      List<JoinNode> joins = new ArrayList<>();
      for (JoinNode join : enumerated) {
        joins.add(new JoinNode(JoinType.CROSS_JOIN, transform(outer), join));
      }
      return joins;
    }
  }

  private LogicalNode findBestPlan(Collection<JoinNode> candidates, JoinNode bestCandidate) {
    LogicalNode plan;
    JoinNode bestPlan = bestCandidate;
    for (JoinNode candidate : candidates) {
      if (bestPlan == null) {
        bestPlan = candidate;
        continue;
      }
      plan = candidate;
      //plan = pushdownSelection(candidate);
      //plan = pushdownProjection(plan);
      if (computeCost(plan) < computeCost(bestPlan)) {
        bestPlan = (JoinNode) plan;
      }
    }

    return bestPlan;
  }

  private void printJoinOrder(JoinNode joinNode) {
    traverseJoinNode(joinNode);
  }

  private void traverseJoinNode(LogicalNode node) {
    if (node.getType() == ExprType.JOIN) {
      JoinNode join = (JoinNode) node;
      System.out.print("(");
      traverseJoinNode(join.getOuterNode());
      System.out.print(",");
      traverseJoinNode(join.getInnerNode());
      System.out.print(")");
    } else if (node.getType() == ExprType.SCAN) {
      ScanNode scan = (ScanNode) node;
      System.out.print(scan.getTableId());
    }
  }

  private LogicalNode createExplicitJoinPlan(Join join) {
    return null;
  }

  public double computeCost(LogicalNode optimized) {
    return computeCostRecursive(optimized);
  }

  private double computeCostRecursive(LogicalNode plan) {
    switch (plan.getType()) {
      case JOIN:
        JoinNode join = (JoinNode) plan;
        double leftCost = computeCostRecursive(join.getOuterNode());
        double rightCost = computeCostRecursive(join.getInnerNode());
        return rightCost + (0.5 * leftCost) + (rightCost * 0.5);

      case SCAN:
        ScanNode scanNode = (ScanNode) plan;
        TableDesc desc = catalog.getTableDesc(scanNode.getTableId());
        return desc.getMeta().getStat().getNumBytes();

      default:
        return 0;
    }
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
