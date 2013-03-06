package tajo.optimizer.annotated;

public interface LogicalOpVisitor {
  boolean accept(LogicalOp node);
  void visit(LogicalOp node);
}
