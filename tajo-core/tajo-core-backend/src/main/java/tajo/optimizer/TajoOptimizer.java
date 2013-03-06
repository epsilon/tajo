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
import tajo.catalog.SortSpec;
import tajo.catalog.TableDesc;
import tajo.datum.DatumFactory;
import tajo.engine.eval.*;
import tajo.engine.eval.EvalNode.Type;
import tajo.engine.parser.QueryBlock;
import tajo.optimizer.annotated.*;

public class TajoOptimizer extends AbstractOptimizer {
  private CatalogService catalog;
  private QueryRewriteEngine rewriteEngine;

  public TajoOptimizer(CatalogService catalog) {
    this.catalog = catalog;
    this.rewriteEngine = new QueryRewriteEngine(catalog);
  }

  private static String ROOT_BLOCK="$ROOT";

  @Override
  public LogicalPlan optimize(Expr algebra) throws VerifyException {

    LogicalPlan plan = new LogicalPlan();

    verify(algebra);

    LogicalOp root = transform(plan, ROOT_BLOCK, algebra);
    plan.setRoot(root);

    LogicalPlan rewritten = rewriteEngine.rewrite(plan);

    LogicalPlan optimized = findBestJoinOrder(rewritten);

    return optimized;
  }

  public LogicalOp transform(LogicalPlan plan, String blockId, Expr expr) throws VerifyException {
    LogicalOp childOp;
    LogicalOp left;
    LogicalOp right;
    switch (expr.getType()) {

      case Projection:
        Projection projection = (Projection) expr;
        plan.addProjection(blockId, projection);
        childOp = transform(plan, blockId, projection.getChild());

        ProjectionOp projectionOp = createProjectionOp(plan, blockId, projection);
        projectionOp.setChildOp(childOp);
        connect(projectionOp, childOp);
        plan.add(blockId, projectionOp);

        return projectionOp;

      case Sort:
        Sort sort = (Sort) expr;
        childOp = transform(plan, blockId, sort.getChild());
        SortOp sortOp = createSortOp(plan, blockId, sort);
        connect(sortOp, childOp);
        plan.add(blockId, sortOp);
        return sortOp;

      case Aggregation:
        Aggregation aggregation = (Aggregation) expr;
        childOp = transform(plan, blockId, aggregation.getChild());
        AggregationOp aggregationOp = createAggregationOp(plan, blockId, aggregation);
        connect(aggregationOp, childOp);
        plan.add(blockId, aggregationOp);
        return aggregationOp;

      case RelationList:
        RelationList relationList = (RelationList) expr;
        childOp = createRelationListNode(plan, blockId, relationList);
        return childOp;

      case Filter:
        Selection selection = (Selection) expr;
        childOp = transform(plan, blockId, selection.getChild());
        EvalNode searchCondition = createEvalTree(plan, blockId, selection.getQual());
        FilterOp filterOp = plan.createOperator(FilterOp.class);

        EvalNode simplified = AlgebraicUtil.simplify(searchCondition);
        EvalNode [] cnf = EvalTreeUtil.getConjNormalForm(simplified);
        filterOp.setQual(cnf);
        filterOp.setInSchema(childOp.getOutSchema());
        filterOp.setOutSchema(childOp.getOutSchema());
        connect(filterOp, childOp);
        plan.add(blockId, filterOp);
        return filterOp;

      case Join:
        Join join = (Join) expr;

        left = transform(plan, blockId, join.getLeft());
        right = transform(plan, blockId, join.getRight());

        JoinOp joinOp = plan.createOperator(JoinOp.class);
        connect(joinOp, left, right);
        plan.add(blockId, joinOp);
        return joinOp;

      case Relation:
        Relation relation = (Relation) expr;
        verifyRelation(relation);
        TableDesc desc = catalog.getTableDesc(relation.getName());
        RelationOp relationOp = plan.createOperator(RelationOp.class);
        if (relation.hasAlias()) {
          relationOp.init(desc.getMeta(), relation.getName(), relation.getAlias());
        } else {
          relationOp.init(desc.getMeta(), relation.getName());
        }
        plan.add(blockId, relationOp);
        return relationOp;

      // the below will has separate query blocks
      case ScalarSubQuery:

      case TableSubQuery:
        TableSubQuery tableSubQuery = (TableSubQuery) expr;
        String subBlockId = tableSubQuery.getCanonicalName();
        childOp = transform(plan, subBlockId, tableSubQuery.getSubQuery());
        TableSubQueryOp subQueryOp = plan.createOperator(TableSubQueryOp.class);
        subQueryOp.init(childOp, tableSubQuery.getName());
        plan.add(blockId, subQueryOp);
        return subQueryOp;

      case Union:

      case Intersect:

      case Except:
    }

    return null;
  }

  public static void connect(UnaryOp parent, LogicalOp op) {
    parent.setChildOp(op);
    op.setParentOp(parent);
  }

  public static void connect(BinaryOp parent, LogicalOp leftOp, LogicalOp rightOp) {
    parent.setLeftOp(leftOp);
    leftOp.setParentOp(parent);
    parent.setRightOp(rightOp);
    rightOp.setParentOp(parent);
  }

  private AggregationOp createAggregationOp(LogicalPlan plan, String blockId,
                                            Aggregation aggregation) throws VerifyException {
    Aggregation.GroupElement [] groupElements = aggregation.getGroupSet();
    QueryBlock.GroupElement annotatedElements [] = new QueryBlock.GroupElement[groupElements.length];
    for (int i = 0; i < groupElements.length; i++) {
      annotatedElements[i] = new QueryBlock.GroupElement(
          groupElements[i].getType(), annotateColumnRef(plan, blockId, groupElements[i].getColumns()));
    }
    AggregationOp aggregationOp = plan.createOperator(AggregationOp.class);
    aggregationOp.init(annotatedElements, annotateTargets(plan, blockId, aggregation.getTargets()));
    return aggregationOp;
  }

  private Column [] annotateColumnRef(LogicalPlan plan, String blockId,
                                      ColumnReferenceExpr[] columnRefs)
      throws VerifyException {
    Column [] columns = new Column[columnRefs.length];
    for (int i = 0; i < columnRefs.length; i++) {
      columns[i] = plan.findColumn(blockId, columnRefs[i]);
    }

    return columns;
  }

  private SortOp createSortOp(LogicalPlan plan, String blockId, Sort sort) throws VerifyException {
    SortSpec [] annotatedSortSpecs = new SortSpec[sort.getSortSpecs().length];

    Column column;
    Sort.SortSpec[] sortSpecs = sort.getSortSpecs();
    for (int i = 0; i < sort.getSortSpecs().length; i++) {
      column = plan.findColumn(blockId, sortSpecs[i].getKey());
      annotatedSortSpecs[i] = new SortSpec(column, sortSpecs[i].isAscending(),
          sortSpecs[i].isNullFirst());
    }

    SortOp sortOp = plan.createOperator(SortOp.class);
    sortOp.init(annotatedSortSpecs);
    return sortOp;
  }

  public static ProjectionOp createProjectionOp(LogicalPlan plan, String blockId, Projection projection)
      throws VerifyException {
    Target [] targets = projection.getTargets();
    QueryBlock.Target [] annotateTargets = annotateTargets(plan, blockId, targets);

    ProjectionOp projectionOp = plan.createOperator(ProjectionOp.class);
    projectionOp.init(annotateTargets);
    return projectionOp;
  }

  static QueryBlock.Target [] annotateTargets(LogicalPlan plan, String blockId,
                                               Target [] targets) throws VerifyException {
    QueryBlock.Target annotatedTargets [] = new QueryBlock.Target[targets.length];

    for (int i = 0; i < targets.length; i++) {
      annotatedTargets[i] = createTarget(plan, blockId, targets[i]);
    }
    return annotatedTargets;
  }

  public static QueryBlock.Target createTarget(LogicalPlan plan, String blockId, Target target)
      throws VerifyException {
    if (target.hasAlias()) {
      return new QueryBlock.Target(createEvalTree(plan, blockId, target.getExpr()), target.getAlias());
    } else {
      return new QueryBlock.Target(createEvalTree(plan, blockId, target.getExpr()));
    }
  }

  private RelationListOp createRelationListNode(LogicalPlan plan, String blockId, RelationList expr)
      throws OptimizationException, VerifyException {

    RelationListOp relationListOp = plan.createOperator(RelationListOp.class);

    RelationOp [] relations = new RelationOp[expr.size()];
    Expr [] exprs = expr.getRelations();
    for (int i = 0; i < expr.size(); i++) {
      relations[i] = (RelationOp) transform(plan, blockId, exprs[i]);
      relations[i].setParentOp(relationListOp);
    }

    relationListOp.init(relations);
    plan.add(blockId, relationListOp);
    return relationListOp;
  }

  private LogicalPlan findBestJoinOrder(LogicalPlan plan) {
    RelationListOp relationList = (RelationListOp) OptimizerUtil.findTopNodeFromRootBlock(plan,
        OpType.RelationList);
    if (relationList != null) {
      JoinOrderAlgorithm algorithm = new GreedyHeuristic(catalog);
      algorithm.findBestOrder(plan);
    }

    return plan;
  }

  @Override
  void verify(Expr algebra) throws OptimizationException {
  }

  private void verifyRelation(Relation relation) throws VerifyException {
    if (!catalog.existsTable(relation.getName())) {
      throw new VerifyException("No such relation: " + relation.getName());
    }
  }

  public static EvalNode createEvalTree(LogicalPlan plan, String blockId, final Expr expr)
      throws VerifyException {
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
        LikeExpr like = (LikeExpr) expr;
        FieldEval field = (FieldEval) createEvalTree(plan, blockId, like.getColumnRef());
        ConstEval pattern = (ConstEval) createEvalTree(plan, blockId, like.getPattern());
        return new LikeEval(like.isNot(), field, pattern);

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
            createEvalTree(plan, blockId, bin.getLeft()), createEvalTree(plan, blockId, bin.getRight()));

      // others
      case Column:
        ColumnReferenceExpr columnRef = (ColumnReferenceExpr) expr;
        Column column;
        if (columnRef.hasRelationName()) {
          column = plan.findColumn(blockId, columnRef.getRelationName(), columnRef.getName());
        } else {
          column = plan.findColumn(blockId, columnRef.getName());
        }
        return new FieldEval(column);

      case Function:
        break;


      case CaseWhen:
        break;

      case ScalarSubQuery:
        ScalarSubQuery scalarSubQuery = (ScalarSubQuery) expr;

      default:
    }
    return null;
  }

  private static Type exprTypeToEvalType(tajo.algebra.ExprType type) throws VerifyException {
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
