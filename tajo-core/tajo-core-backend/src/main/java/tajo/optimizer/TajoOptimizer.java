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
import tajo.catalog.Column;
import tajo.catalog.TableDesc;
import tajo.datum.DatumFactory;
import tajo.engine.eval.*;
import tajo.engine.eval.EvalNode.Type;
import tajo.engine.parser.QueryBlock;
import tajo.engine.planner.logical.ExprType;
import tajo.engine.planner.logical.JoinNode;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.logical.ScanNode;
import tajo.optimizer.annotated.*;
import tajo.util.TUtil;

import java.util.*;

public class TajoOptimizer extends AbstractOptimizer {
  private CatalogService catalog;
  private QueryRewriteEngine rewriteEngine;

  private class OptimizationContext {

  }

  public TajoOptimizer(CatalogService catalog) {
    this.catalog = catalog;
    this.rewriteEngine = new QueryRewriteEngine(catalog);
  }

  @Override
  public LogicalPlan optimize(Expr algebra) throws OptimizationException {

    LogicalPlan plan = new LogicalPlan();

    verify(algebra);

    LogicalOp root = transform(plan, algebra);
    plan.setRoot(root);
    LogicalPlan rewritten = rewriteEngine.rewrite(plan);

    return plan;
  }

  public LogicalOp transform(LogicalPlan plan, Expr expr) throws OptimizationException {
    LogicalOp child;
    LogicalOp left;
    LogicalOp right;
    switch (expr.getType()) {
      case Projection:
        Projection projection = (Projection) expr;
        child = transform(plan, projection.getChild());
        ProjectionOp projectionOp = createProjectionOp(plan, projection);
        projectionOp.setChild(child);
        plan.add(projectionOp);
        return projectionOp;

      case RelationList:
        RelationList relationList = (RelationList) expr;
        child = createRelationListNode(plan, relationList);
        return child;

      case Selection:
        Selection selection = (Selection) expr;
        child = transform(plan, selection.getChild());
        EvalNode searchCondition = createEvalTree(plan, selection.getQual());
        SelectionOp selectionOp = plan.createLogicalOp(SelectionOp.class);

        EvalNode simplified = AlgebraicUtil.simplify(searchCondition);
        EvalNode [] cnf = EvalTreeUtil.getConjNormalForm(simplified);
        selectionOp.setQual(cnf);
        selectionOp.setChild(child);
        plan.add(selectionOp);
        return selectionOp;

      case Join:
        Join join = (Join) expr;

        left = transform(plan, join.getLeft());
        right = transform(plan, join.getRight());

        JoinOp joinOp = plan.createLogicalOp(JoinOp.class);
        joinOp.setOuter(left);
        joinOp.setInner(right);
        plan.add(joinOp);
        return joinOp;

      case Relation:
        Relation relation = (Relation) expr;
        verifyRelation(relation);
        TableDesc desc = catalog.getTableDesc(relation.getName());
        RelationOp relationOp = plan.createLogicalOp(RelationOp.class);
        relationOp.init(relation, desc.getMeta().getSchema());
        plan.add(relationOp);
        return relationOp;

      // the below will has separate query blocks
      case ScalarSubQuery:

      case TableSubQuery:
        TableSubQuery tableSubQuery = (TableSubQuery) expr;
        child = transform(plan, tableSubQuery.getSubQuery());
        TableSubQueryOp subQueryOp = plan.createLogicalOp(TableSubQueryOp.class);
        subQueryOp.init(child, tableSubQuery.getName());
        plan.add(subQueryOp);
        return subQueryOp;

      case Union:

      case Intersect:

      case Except:
    }

    return null;
  }

  private ProjectionOp createProjectionOp(LogicalPlan plan, Projection projection) throws VerifyException {
    Target [] exprs = projection.getTargets();
    QueryBlock.Target targets [] = new QueryBlock.Target[exprs.length];

    for (int i = 0; i < exprs.length; i++) {
      targets[i] = createTarget(plan, exprs[i]);
    }

    ProjectionOp projectionOp = plan.createLogicalOp(ProjectionOp.class);
    projectionOp.init(targets);
    return projectionOp;
  }

  public QueryBlock.Target createTarget(LogicalPlan plan, Target target) throws VerifyException {
    if (target.hasAlias()) {
      return new QueryBlock.Target(createEvalTree(plan, target.getExpr()), target.getAlias());
    } else {
      return new QueryBlock.Target(createEvalTree(plan, target.getExpr()));
    }
  }

  private RelationListOp createRelationListNode(LogicalPlan plan, RelationList expr)
      throws OptimizationException {
    LogicalOp [] relations = new LogicalOp[expr.size()];
    Expr [] exprs = expr.getRelations();
    for (int i = 0; i < expr.size(); i++) {
      relations[i] = transform(plan, exprs[i]);
    }

    RelationListOp relationListOp = plan.createLogicalOp(RelationListOp.class);
    relationListOp.init(relations);
    plan.add(relationListOp);
    return relationListOp;
  }

  private LogicalNode createImplicitJoinPlan(RelationList expr) throws OptimizationException {
    Set<Expr> relSet = TUtil.newHashSet(expr.getRelations());
    Set<Expr> nextRemain;
    Set<JoinNode> enumerated = new HashSet<JoinNode>();
    JoinNode bestPlan = null;
    Collection<JoinNode> candidate;
    int i = 1;
    JoinEnumerator enuemrator = new RightDeepEnumerator();
    nextRemain = new HashSet<Expr>(relSet);
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
      Set<JoinNode> enumerated = new HashSet<JoinNode>();
      Set<Expr> nextRemain;
      for (Expr rel : exprs) {
        nextRemain = new HashSet<Expr>(exprs);
        nextRemain.remove(rel);
        enumerated.addAll(enumerateLeftDeepJoin(nextRemain, rel));
      }

      return enumerated;
    }

    private Collection<JoinNode> enumerateLeftDeepJoin(Set<Expr> remain, Expr rel)
        throws OptimizationException {
      List<JoinNode> enumerated = new ArrayList<JoinNode>();
      if (remain.size() == 1) {
//        enumerated.add(new JoinNode(JoinType.CROSS_JOIN,
//            transform(rel), transform(remain.iterator().next())));
      } else {
        Set<Expr> nextRemain;
        for (Expr next : remain) {
          nextRemain = new HashSet<Expr>(remain);
          nextRemain.remove(next);
          enumerated.addAll(createLeftDeepJoin(enumerateLeftDeepJoin(nextRemain, next), rel));
        }
      }

      return enumerated;
    }

    private List<JoinNode> createLeftDeepJoin(Collection<JoinNode> enumerated, Expr inner) throws OptimizationException {
      List<JoinNode> joins = new ArrayList<JoinNode>();
      for (JoinNode join : enumerated) {
        //joins.add(new JoinNode(JoinType.CROSS_JOIN, join, transform(inner)));
      }
      return joins;
    }
  }

  private class RightDeepEnumerator implements JoinEnumerator {

    @Override
    public Collection<JoinNode> enumerate(Set<Expr> exprs) throws OptimizationException {
      Set<JoinNode> enumerated = new HashSet<JoinNode>();
      Set<Expr> nextRemain;
      for (Expr rel : exprs) {
        nextRemain = new HashSet<Expr>(exprs);
        nextRemain.remove(rel);
        enumerated.addAll(enumerateRightDeepJoin(rel, nextRemain));
      }

      return enumerated;
    }

    private Collection<JoinNode> enumerateRightDeepJoin(Expr rel, Set<Expr> remain)
        throws OptimizationException {
      List<JoinNode> enumerated = new ArrayList<JoinNode>();
      if (remain.size() == 1) {
        //enumerated.add(new JoinNode(JoinType.CROSS_JOIN,
          //  transform(rel), transform(remain.iterator().next())));
      } else {
        Set<Expr> nextRemain;
        for (Expr next : remain) {
          nextRemain = new HashSet<Expr>(remain);
          nextRemain.remove(next);
          enumerated.addAll(createRightDeepJoin(rel, enumerateRightDeepJoin(next, nextRemain)));
        }
      }

      return enumerated;
    }

    private List<JoinNode> createRightDeepJoin(Expr outer, Collection<JoinNode> enumerated) throws OptimizationException {
      List<JoinNode> joins = new ArrayList<JoinNode>();
      for (JoinNode join : enumerated) {
        //joins.add(new JoinNode(JoinType.CROSS_JOIN, transform(outer), join));
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

  @Override
  void verify(Expr algebra) throws OptimizationException {
  }

  private void verifyRelation(Relation relation) throws VerifyException {
    if (!catalog.existsTable(relation.getName())) {
      throw new VerifyException("No such relation: " + relation.getName());
    }
  }

  public EvalNode createEvalTree(LogicalPlan plan, final Expr expr) throws VerifyException {
    switch(expr.getType()) {

      // constants
      case Literal:
        LiteralExpr literal = (LiteralExpr) expr;
        switch (literal.getValueType()) {
          case String:
            return new ConstEval(DatumFactory.createString(literal.getValue()));
          case Unsigned_Integer:
            return new ConstEval(DatumFactory.createInt(literal.getValue()));
          case Unsigned_Large_Integer:
            return new ConstEval(DatumFactory.createLong(literal.getValue()));
          case Unsigned_Float:
            return new ConstEval(DatumFactory.createFloat(literal.getValue()));
          default:
            throw new VerifyException("Unsupported type: " + literal.getValueType());
        }

      // unary expression
      case Not:
        break;

      // binary expressions
      case Like:
        break;

      case Is:
        break;

      case And:
      case Or:
      case Equals:
      case NotEquals:
      case LessThan:
      case LessThanOrEquals:
      case GreaterThan:
      case GreaterThanOrEquals:
      case Plus:
      case Minus:
      case Multiply:
      case Divide:
      case Mod:
        BinaryOperator bin = (BinaryOperator) expr;
        return new BinaryEval(exprTypeToEvalType(expr.getType()),
            createEvalTree(plan, bin.getLeft()), createEvalTree(plan, bin.getRight()));

      // others
      case Column:
        ColumnReferenceExpr columnRef = (ColumnReferenceExpr) expr;
        Column column;
        if (columnRef.hasRelationName()) {
          column = plan.getColumn(columnRef.getRelationName(), columnRef.getName());
        } else {
          column = plan.getColumn(columnRef.getName());
        }
        return new FieldEval(column);

      case Function:
        break;


      case CaseWhen:
        break;

      default:
    }
    return null;
  }

  private Type exprTypeToEvalType(tajo.algebra.ExprType type) throws VerifyException {
    switch (type) {
      case And: return Type.AND;
      case Or: return Type.OR;
      case Equals: return Type.EQUAL;
      case NotEquals: return Type.NOT_EQUAL;
      case LessThan: return Type.LTH;
      case LessThanOrEquals: return Type.LEQ;
      case GreaterThan: return Type.GTH;
      case GreaterThanOrEquals: return Type.GEQ;
      case Plus: return Type.PLUS;
      case Minus: return Type.MINUS;
      case Multiply: return Type.MULTIPLY;
      case Divide: return Type.DIVIDE;
      case Mod: return Type.MODULAR;
      case Column: return Type.FIELD;
      case Function: return Type.FUNCTION;
      default: throw new VerifyException("Unsupported type: " + type);
    }
  }


  public EvalNode createBinaryEvalNode(Expr expr) {
    return null;
  }
}
