package tajo.optimizer;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.junit.Test;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.proto.CatalogProtos;

public class TestJson {

  @Test
  public void test() {
    CatalogProtos.DataType type;

    Column col = new Column("test", CatalogProtos.DataType.DATE);
    Gson gson = new GsonBuilder()
        .excludeFieldsWithoutExposeAnnotation()
        .setPrettyPrinting()
        .create();
    col.mergeProtoToLocal();
    System.out.println(gson.toJson(col));

    Schema schema = new Schema(
        new Column[]{
            new Column("col1", CatalogProtos.DataType.INT),
            new Column("col2", CatalogProtos.DataType.LONG),
            new Column("col3", CatalogProtos.DataType.DATE)
        }
    );

    schema.mergeProtoToLocal();
    System.out.println(gson.toJson(schema));
  }
}
