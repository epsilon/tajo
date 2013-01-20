package tajo.algebra;

import tajo.engine.parser.QueryBlock.Target;
import java.util.Arrays;

public class ProjectionExpr extends UnaryExpr implements Cloneable {
  /**
   * the targets are always filled even if the query is 'select *'
   */
  private Target [] targets;

	public ProjectionExpr(Target[] targets) {
		super(ExprType.PROJECTION);
		this.targets = targets;
	}
	
	public Target [] getTargets() {
	  return this.targets;
	}
	
	public String toString() {
	  StringBuilder sb = new StringBuilder(type.getName());
	  sb.append(getSubExpr());
    return sb.toString();
	}
	
	@Override
  public boolean equals(Object obj) {
	  if (obj instanceof ProjectionExpr) {
	    ProjectionExpr other = (ProjectionExpr) obj;
	    
	    boolean b1 = super.equals(other);
	    boolean b2 = Arrays.equals(targets, other.targets);
	    boolean b3 = subExpr.equals(other.subExpr);
	    
	    return b1 && b2 && b3;
	  } else {
	    return false;
	  }
	}
	
	@Override
  public Object clone() throws CloneNotSupportedException {
	  ProjectionExpr projNode = (ProjectionExpr) super.clone();
	  projNode.targets = targets.clone();
	  
	  return projNode;
	}
}
