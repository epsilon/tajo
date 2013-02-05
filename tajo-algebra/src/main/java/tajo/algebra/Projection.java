package tajo.algebra;

import java.util.Arrays;

public class Projection extends UnaryOperator implements Cloneable {
  private boolean all;

  private Target [] targets;

  public Projection() {
    super(ExprType.Projection);
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
	  if (obj instanceof Projection) {
	    Projection other = (Projection) obj;
	    
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
