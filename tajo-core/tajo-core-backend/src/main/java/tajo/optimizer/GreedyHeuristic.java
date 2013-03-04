package tajo.optimizer;

import tajo.catalog.CatalogService;
import tajo.optimizer.annotated.*;

import java.util.*;

public class GreedyHeuristic implements JoinOrderAlgorithm {

  private CatalogService catalog;
  public GreedyHeuristic(CatalogService catalog) {
    this.catalog = catalog;
  }

  @Override
  public LogicalPlan findBestOrder(LogicalPlan plan) {
    SelectionOp selection = (SelectionOp) OptimizerUtil.findTopNode(plan, OpType.Selection);

    RelationListOp relationList = (RelationListOp) OptimizerUtil.findTopNode(plan,
        OpType.RelationList);

    SortedSet<LogicalOp> relationSet = new TreeSet<LogicalOp>(new RelationComparator());
    for (LogicalOp op : relationList.getRelations()) {
      relationSet.add(op);
    }

    JoinOp prev = plan.createLogicalOp(JoinOp.class);
    LogicalOp first = relationSet.first();
    prev.setOuter(first);
    relationSet.remove(first);
    JoinOp join;

    while(true) {
      first = relationSet.first();
      relationSet.remove(first);
      prev.setInner(first);

      if (!relationSet.isEmpty()) {
        join = plan.createLogicalOp(JoinOp.class);
        join.setOuter(prev);
        prev = join;
      } else {
        break;
      }
    }

    TajoOptimizer.printJoinOrder(prev);


    return null;
  }

  private LogicalOp createLeftDeepJoin(LogicalPlan plan, SortedSet<LogicalOp> relationSet) {
    if (relationSet.size() == 2) {
      LogicalOp op = relationSet.first();
      relationSet.remove(op);
      return op;
    } else {
      JoinOp joinOp = plan.createLogicalOp(JoinOp.class);
      joinOp.setOuter(createLeftDeepJoin(plan, relationSet));
      LogicalOp first = relationSet.first();
      joinOp.setInner(first);
      relationSet.remove(first);

      return joinOp;
    }
  }

  private LogicalOp createRightDeepJoin(LogicalPlan plan, SortedSet<LogicalOp> relationSet) {
    if (relationSet.size() == 2) {
      LogicalOp op = relationSet.first();
      relationSet.remove(op);
      return op;
    } else {
      JoinOp joinOp = plan.createLogicalOp(JoinOp.class);
      LogicalOp first = relationSet.first();
      joinOp.setOuter(first);
      relationSet.remove(first);
      joinOp.setInner(createRightDeepJoin(plan, relationSet));
      return joinOp;
    }
  }

  private long getCost(LogicalOp op) {
    if (op instanceof RelationOp) {
      RelationOp rel = (RelationOp) op;
      return rel.getMeta().getStat().getNumBytes();
    } else {
     throw new IllegalStateException();
    }
  }

  class RelationComparator implements Comparator<LogicalOp> {
    @Override
    public int compare(LogicalOp o1, LogicalOp o2) {
      if (getCost(o1) < getCost(o2)) {
        return -1;
      } else if (getCost(o1) > getCost(o2)) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
