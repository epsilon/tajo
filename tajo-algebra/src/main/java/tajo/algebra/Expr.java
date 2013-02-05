package tajo.algebra;

import com.google.gson.*;

import java.lang.reflect.Type;

public abstract class Expr implements JsonSerializable {
  protected ExprType type;

  Expr() {}

	public Expr(ExprType type) {
		this.type = type;
	}
	
	public ExprType getType() {
		return this.type;
	}
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof Expr) {
	    Expr other = (Expr) obj;

      return this.type == other.type;
	  } else {
	    return false;
	  }
	}

  @Override
  public String toString() {
    return toJson();
  }

  public String toJson() {
    return JsonHelper.toJson(this);
  }

  static class JsonSerDer
      implements JsonSerializer<Expr>, JsonDeserializer<Expr> {

    @Override
    public Expr deserialize(JsonElement json, Type typeOfT,
                                    JsonDeserializationContext context)
        throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      String operator = jsonObject.get("type").getAsString();
      return context.deserialize(json, ExprType.valueOf(operator).getBaseClass());
    }


    @Override
    public JsonElement serialize(Expr src, Type typeOfSrc,
                                 JsonSerializationContext context) {
      return context.serialize(src, src.type.getBaseClass());
    }
  }
}
