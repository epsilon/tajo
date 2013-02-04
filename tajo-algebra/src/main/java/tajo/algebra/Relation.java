package tajo.algebra;

public class Relation extends RelationalOp {
  private String relation;
  private String alias;

  public Relation(String relation) {
    super(OperatorType.Relation);
    this.relation = relation;
  }

  public String getName() {
    return relation;
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
}
