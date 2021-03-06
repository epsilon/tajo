package tajo.engine.planner.logical;

import tajo.engine.json.GsonCreator;

/**
 * @author Hyunsik Choi
 */
public class LogicalRootNode extends UnaryNode implements Cloneable {
  public LogicalRootNode() {
    super(ExprType.ROOT);
  }
  
  public String toString() {
    return "Logical Plan Root\n\n" + getSubNode().toString();
  }
  
  @Override
  public String toJSON() {
    return GsonCreator.getInstance().toJson(this, LogicalNode.class);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof LogicalRootNode) {
      LogicalRootNode other = (LogicalRootNode) obj;
      boolean b1 = super.equals(other);
      boolean b2 = subExpr.equals(other.subExpr);
      
      return b1 && b2;
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }
}
