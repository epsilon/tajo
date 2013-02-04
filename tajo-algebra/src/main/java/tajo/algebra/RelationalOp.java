package tajo.algebra;

import com.google.gson.*;

import java.lang.reflect.Type;

public abstract class RelationalOp implements JsonSerializable {
  protected OperatorType operator;

  RelationalOp() {}

	public RelationalOp(OperatorType operator) {
		this.operator = operator;
	}
	
	public OperatorType getOperator() {
		return this.operator;
	}
	
	@Override
	public boolean equals(Object obj) {
	  if (obj instanceof RelationalOp) {
	    RelationalOp other = (RelationalOp) obj;

      return this.operator == other.operator;
	  } else {
	    return false;
	  }
	}

  @Override
  public String toString() {
    return toJson();
  }

  public abstract String toJson();

  public static OperatorType stringToOperator(String str) {
    OperatorType opType = null;
    switch (str) {
      case "Sort": opType = OperatorType.Sort; break;
      case "Join": opType = OperatorType.Join; break;
      case "Aggregation": opType = OperatorType.Aggregation; break;
      case "Except": opType = OperatorType.Except; break;
      case "Intersect": opType = OperatorType.Intersect; break;
      case "Limit": opType = OperatorType.Limit; break;
      case "Projection": opType = OperatorType.Projection; break;
      case "Relation": opType = OperatorType.Relation; break;
      case "Rename": opType = OperatorType.Rename; break;
      case "Union": opType = OperatorType.Union; break;
      default: new RuntimeException("Unknown OperatorType: " + str);
    }
    return opType;
  }

  public static Class stringToClass(String str) {
    Class clazz = null;
    switch (str) {
      case "Selection": clazz = SelectionOp.class; break;
      case "Sort": clazz = SortOp.class; break;
      case "Join": clazz = JoinOp.class; break;
      case "Aggregation": clazz = AggregationOp.class; break;
      case "Except": break;
      case "Intersect": break;
      case "Limit": break;
      case "Projection": clazz = ProjectionOp.class; break;
      case "Relation": clazz = Relation.class; break;
      case "Rename": break;
      case "Union": break;
      default: new RuntimeException("Unknown OperatorType: " + str);
    }
    return clazz;
  }

  public static Class operatorToClass(OperatorType opType) {
    Class clazz = null;
    switch (opType) {
      case Selection: clazz = SelectionOp.class; break;
      case Sort: clazz = SortOp.class; break;
      case Join: clazz = JoinOp.class; break;
      case Aggregation: clazz = AggregationOp.class; break;
      case Except: break;
      case Intersect: break;
      case Limit: break;
      case Projection: break;
      case Relation: clazz = Relation.class; break;
      case Rename: break;
      case Union: break;
      default: new RuntimeException("Unknown OperatorType: " + opType);
    }
    return clazz;
  }

  static class JsonSerDer
      implements JsonSerializer<RelationalOp>, JsonDeserializer<RelationalOp> {

    @Override
    public RelationalOp deserialize(JsonElement json, Type typeOfT,
                                    JsonDeserializationContext context)
        throws JsonParseException {
      JsonObject jsonObject = json.getAsJsonObject();
      String operator = jsonObject.get("operator").getAsString();
      return context.deserialize(json, stringToClass(operator));
    }


    @Override
    public JsonElement serialize(RelationalOp src, Type typeOfSrc,
                                 JsonSerializationContext context) {
      return context.serialize(src, operatorToClass(src.operator));
    }
  }
}
