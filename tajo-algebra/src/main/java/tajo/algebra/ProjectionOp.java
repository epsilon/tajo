package tajo.algebra;

import java.util.Arrays;

public class ProjectionOp extends UnaryOp implements Cloneable {
  private boolean all;

  private Target [] targets;

  public ProjectionOp() {
    super(OperatorType.Projection);
  }

  public void setAll() {
    all = true;
  }

  public boolean isAllProjected() {
    return all;
  }
	
	public Target [] getTargets() {
	  return this.targets;
	}

  public void setTargets(Target [] targets) {
    this.targets = targets;
  }
	
	@Override
  public boolean equals(Object obj) {
	  if (obj instanceof ProjectionOp) {
	    ProjectionOp other = (ProjectionOp) obj;
	    
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
