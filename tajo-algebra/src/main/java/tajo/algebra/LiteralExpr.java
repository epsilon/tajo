package tajo.algebra;

public class LiteralExpr extends Expr {
  private String value;
  private LiteralType type;

  public static enum LiteralType {
    String,
    Unsigned_Integer,
    Unsigned_Float,
    Unsigned_Large_Integer,
  }

  public LiteralExpr(String value, LiteralType type) {
    super(ExpressionType.Literal);
    this.value = value;
    this.type = type;
  }

  public String getValue() {
    return this.value;
  }

  public LiteralType getType() {
    return this.type;
  }
}
