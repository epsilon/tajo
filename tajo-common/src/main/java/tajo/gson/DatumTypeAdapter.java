package tajo.gson;

import com.google.gson.*;
import tajo.datum.Datum;

import java.lang.reflect.Type;

public class DatumTypeAdapter implements JsonSerializer<Datum>, JsonDeserializer<Datum> {

  @Override
  public Datum deserialize(JsonElement json, Type typeOfT,
      JsonDeserializationContext context) throws JsonParseException {
    JsonObject jsonObject = json.getAsJsonObject();
    String className = jsonObject.get("classname").getAsJsonPrimitive().getAsString();
    
    Class clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
      throw new JsonParseException(e);
    }
    return context.deserialize(jsonObject.get("property"), clazz);
  }

  @Override
  public JsonElement serialize(Datum src, Type typeOfSrc,
      JsonSerializationContext context) {
    JsonObject jsonObj = new JsonObject();
    String className = src.getClass().getCanonicalName();
    jsonObj.addProperty("classname", className);
    JsonElement jsonElem = context.serialize(src);
    jsonObj.add("property", jsonElem);
    return jsonObj;
  }

}
