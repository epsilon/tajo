package tajo.optimizer;

import tajo.engine.planner.logical.LogicalNode;

public class ExecutionBlock {
  int id;
  LogicalNode block;

  public int getId() {
    return this.id;
  }
}
