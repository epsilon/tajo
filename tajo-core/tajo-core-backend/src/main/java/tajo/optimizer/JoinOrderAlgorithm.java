package tajo.optimizer;

import tajo.optimizer.annotated.LogicalPlan;

public interface JoinOrderAlgorithm {
  public LogicalPlan findBestOrder(LogicalPlan plan);
}
