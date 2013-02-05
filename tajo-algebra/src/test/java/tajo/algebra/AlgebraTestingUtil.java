package tajo.algebra;

import static junit.framework.Assert.assertEquals;

public class AlgebraTestingUtil {
  public static void testJsonSerializer(JsonSerializable obj) {
    Class clazz = obj.getClass();
    String json = obj.toJson();
    JsonSerializable restored = (JsonSerializable) JsonHelper.fromJson(json, clazz);
    assertEquals(json, restored.toJson());
  }
}
