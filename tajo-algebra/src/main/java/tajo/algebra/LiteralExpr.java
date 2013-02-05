package tajo.algebra;

public class LiteralExpr extends Expr {
  private String value;
  private LiteralType value_type;

  public static enum LiteralType {
    String,
    Unsigned_Integer,
    Unsigned_Float,
    Unsigned_Large_Integer,
  }

  public LiteralExpr(String value, LiteralType value_type) {
    super(ExprType.Literal);
    this.value = value;
    this.value_type = value_type;
  }

  public String getValue() {
    return this.value;
  }

  public LiteralType getValueType() {
    return this.value_type;
  }
}
