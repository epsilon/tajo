package tajo.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import tajo.catalog.proto.CatalogProtos.TableProto;
import tajo.util.FileUtil;

import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class TableUtil {
  public static TableMeta getTableMeta(Configuration conf, Path tablePath) 
      throws IOException {
    TableMetaImpl meta = null;
    
    FileSystem fs = tablePath.getFileSystem(conf);
    
    Path tableMetaPath = new Path(tablePath, ".meta");
    if(!fs.exists(tableMetaPath)) {
      throw new FileNotFoundException(".meta file not found in "+tablePath.toString());
    }
    FSDataInputStream tableMetaIn = 
      fs.open(tableMetaPath);

    TableProto tableProto = (TableProto) FileUtil.loadProto(tableMetaIn, 
      TableProto.getDefaultInstance());
    meta = new TableMetaImpl(tableProto);

    return meta;
  }
}
