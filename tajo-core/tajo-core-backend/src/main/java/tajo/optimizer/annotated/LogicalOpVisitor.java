package tajo.optimizer.annotated;

public interface LogicalOpVisitor {
  void visit(LogicalOp node);
}
