package tajo.algebra;

import tajo.engine.parser.QueryBlock.Target;

import java.util.Arrays;

public class ProjectionExpr extends UnaryOp implements Cloneable {
  /**
   * the targets are always filled even if the query is 'select *'
   */
  private Target [] targets;

	public ProjectionExpr(Target[] targets) {
		super(OperatorType.Projection);
		this.targets = targets;
	}
	
	public Target [] getTargets() {
	  return this.targets;
	}
	
	@Override
  public boolean equals(Object obj) {
	  if (obj instanceof ProjectionExpr) {
	    ProjectionExpr other = (ProjectionExpr) obj;
	    
	    boolean b1 = super.equals(other);
	    boolean b2 = Arrays.equals(targets, other.targets);
	    
	    return b1 && b2;
	  } else {
	    return false;
	  }
	}

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }
}
