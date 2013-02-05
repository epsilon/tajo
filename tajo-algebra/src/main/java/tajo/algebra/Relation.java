package tajo.algebra;

import tajo.util.TUtil;

public class Relation extends Expr {
  private String rel_name;
  private String alias;

  public Relation(String relation) {
    super(ExprType.Relation);
    this.rel_name = relation;
  }

  public String getName() {
    return rel_name;
  }

  public boolean hasAlias() {
    return alias != null;
  }

  public String getAlias() {
    return this.alias;
  }

  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof  Relation) {
      Relation other = (Relation) obj;
      return TUtil.checkEquals(rel_name, other.rel_name) &&
          TUtil.checkEquals(alias, other.alias);
    }

    return false;
  }

  @Override
  public int hashCode() {
    int result = rel_name.hashCode();
    result = 31 * result + (alias != null ? alias.hashCode() : 0);
    return result;
  }
}
