package tajo.algebra;

import tajo.util.TUtil;

import java.util.Set;

public class RelationList extends Expr {
  private Relation [] relations;

  public RelationList(Relation [] relations) {
    super(ExprType.RelationList);
    this.relations = relations;
  }

  public Relation [] getRelations() {
    return this.relations;
  }

  @Override
  public String toString() {
    return toJson();
  }

  @Override
  boolean equalsTo(Expr expr) {
    Set<Relation> thisSet = TUtil.newHashSet(relations);
    RelationList another = (RelationList) expr;
    Set<Relation> anotherSet = TUtil.newHashSet(another.relations);
    return thisSet.equals(anotherSet);
  }
}
