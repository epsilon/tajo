/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.catalog;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;
import org.apache.hadoop.fs.Path;
import tajo.catalog.json.GsonCreator;
import tajo.catalog.proto.CatalogProtos.StoreType;
import tajo.catalog.proto.CatalogProtos.TableDescProto;
import tajo.catalog.proto.CatalogProtos.TableDescProtoOrBuilder;
import tajo.common.ProtoObject;

/**
 * @author Hyunsik Choi
 *
 */
public class TableDescImpl implements TableDesc, ProtoObject<TableDescProto>,
    Cloneable {
  protected TableDescProto proto = TableDescProto.getDefaultInstance();
  protected TableDescProto.Builder builder = null;
  protected boolean viaProto = false;
  
	@Expose protected String tableId;
	@Expose protected Path uri;
	@Expose protected TableMeta meta;
  
	public TableDescImpl() {
		builder = TableDescProto.newBuilder();
	}
	
	public TableDescImpl(String tableId, TableMeta info, Path path) {
		this();
		// tajo deems all identifiers as lowcase characters
	  this.tableId = tableId.toLowerCase();
	  this.meta = info;
	  this.uri = path;	   
	}
	
	public TableDescImpl(String tableId, Schema schema, StoreType type, 
	    Options options, Path path) {
	  this(tableId, new TableMetaImpl(schema, type, options), path);
	}
	
	public TableDescImpl(TableDescProto proto) {
	  this.proto = proto;
	  viaProto = true;
	}
	
	public void setId(String tableId) {
	  setModified();
	  // tajo deems all identifiers as lowcase characters
		this.tableId = tableId.toLowerCase();
	}
	
  public String getId() {
    TableDescProtoOrBuilder p = viaProto ? proto : builder;
    
    if (tableId != null) {
      return this.tableId;
    }
    if (!p.hasId()) {
      return null;
    }
    this.tableId = p.getId();
    
    return this.tableId;
  }
	
	public void setPath(Path uri) {
	  setModified();
		this.uri = uri;
	}
	
  public Path getPath() {
    TableDescProtoOrBuilder p = viaProto ? proto : builder;
    
    if (uri != null) {
      return this.uri;
    }
    if (!proto.hasPath()) {
      return null;
    }
    this.uri = new Path(p.getPath());
    
    return this.uri;
  }
  
  @Override
  public void setMeta(TableMeta info) {
    setModified();
    this.meta = info;
  }
	
	public TableMeta getMeta() {
	  TableDescProtoOrBuilder p = viaProto ? proto : builder;
    
    if (meta != null) {
      return this.meta;
    }
    if (!p.hasMeta()) {
      return null;
    }
    this.meta = new TableMetaImpl(p.getMeta());
	  return this.meta;
	}
	
  public Schema getSchema() {
    return getMeta().getSchema();
  }
	
	public boolean equals(Object object) {
    if(object instanceof TableDescImpl) {
      TableDescImpl other = (TableDescImpl) object;
      
      return this.getProto().equals(other.getProto());
    }
    
    return false;   
  }
	
	public Object clone() throws CloneNotSupportedException {	  
	  TableDescImpl desc = (TableDescImpl) super.clone();
	  mergeProtoToLocal();
	  desc.proto = null;
	  desc.builder = TableDescProto.newBuilder();
	  desc.viaProto = false;
	  desc.tableId = tableId;
	  desc.uri = uri;
	  desc.meta = (TableMeta) meta.clone();
	  
	  return desc;
	}
	
	public String toString() {
	  Gson gson = new GsonBuilder().setPrettyPrinting().
	      excludeFieldsWithoutExposeAnnotation().create();
    return gson.toJson(this);
	}
	
	public String toJSON() {
		mergeProtoToLocal();
		Gson gson = GsonCreator.getInstance();
		
		return gson.toJson(this, TableDesc.class);
	}

  public TableDescProto getProto() {
    if(!viaProto) {
      mergeLocalToBuilder();
      proto = builder.build();
      viaProto = true;
    }
    
    return proto;
  }
  
  private void setModified() {
    if (viaProto && builder == null) {
      builder = TableDescProto.newBuilder(proto);
    }
    viaProto = false;
  }
  
  protected void mergeLocalToBuilder() {
    if (builder == null) {
      builder = TableDescProto.newBuilder(proto);
    }
    
    if (this.tableId != null) {
      builder.setId(this.tableId);
    }
    
    if (this.uri != null) {
      builder.setPath(this.uri.toString());
    }
    
    if (this.meta != null) {
      builder.setMeta(meta.getProto());
    }
  }

  @Override
  public void mergeProtoToLocal() {
	  TableDescProtoOrBuilder p = viaProto ? proto : builder;
	  if (tableId == null && p.hasId()) {
		  tableId = p.getId();
	  }
	  if (uri == null && p.hasPath()) {
		  uri = new Path(p.getPath());
	  }
	  if (meta == null && p.hasMeta()) {
		  meta = new TableMetaImpl(p.getMeta());
      meta.mergeProtoToLocal();
	  }
  }
}