package nta.engine.function;

import nta.catalog.ColumnBase;
import nta.catalog.proto.CatalogProtos.DataType;
import nta.datum.Datum;

public class Aggavg extends Function {

  public Aggavg() {
    super(new ColumnBase[] { new ColumnBase("arg1", DataType.INT) });
  }

  @Override
  public Datum invoke(Datum... data) {
    if (data.length == 1) {
      return data[0];
    } else {
      return data[0].plus(data[1]);
    }
  }

  @Override
  public DataType getResType() {
    return DataType.FLOAT;
  }
}