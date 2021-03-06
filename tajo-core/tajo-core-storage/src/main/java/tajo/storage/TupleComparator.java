/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.storage;

import com.google.common.base.Preconditions;
import tajo.catalog.Schema;
import tajo.catalog.SortSpec;
import tajo.catalog.proto.CatalogProtos.SortSpecProto;
import tajo.common.ProtoObject;
import tajo.datum.Datum;
import tajo.datum.DatumType;
import tajo.storage.index.IndexProtos.TupleComparatorProto;

import java.util.Comparator;

/**
 * The Comparator class for Tuples
 * 
 * @see Tuple
 */
public class TupleComparator implements Comparator<Tuple>, ProtoObject<TupleComparatorProto> {
  private final int[] sortKeyIds;
  private final boolean[] asc;
  @SuppressWarnings("unused")
  private final boolean[] nullFirsts;  

  private Datum left;
  private Datum right;
  private int compVal;

  public TupleComparator(Schema schema, SortSpec[] sortKeys) {
    Preconditions.checkArgument(sortKeys.length > 0, 
        "At least one sort key must be specified.");
    
    this.sortKeyIds = new int[sortKeys.length];
    this.asc = new boolean[sortKeys.length];
    this.nullFirsts = new boolean[sortKeys.length];
    for (int i = 0; i < sortKeys.length; i++) {
      this.sortKeyIds[i] = schema.getColumnId(sortKeys[i].getSortKey().getQualifiedName());
          
      this.asc[i] = sortKeys[i].isAscending();
      this.nullFirsts[i]= sortKeys[i].isNullFirst();
    }
  }

  public TupleComparator(TupleComparatorProto proto) {
    this.sortKeyIds = new int[proto.getSortSpecsCount()];
    this.asc = new boolean[proto.getSortSpecsCount()];
    this.nullFirsts = new boolean[proto.getSortSpecsCount()];

    for (int i = 0; i < proto.getSortSpecsCount(); i++) {
      SortSpecProto sortSepcProto = proto.getSortSpecs(i);
      sortKeyIds[i] = sortSepcProto.getSortColumnId();
      asc[i] = sortSepcProto.getAscending();
      nullFirsts[i] = sortSepcProto.getNullFirst();
    }
  }

  public boolean isAscendingFirstKey() {
    return this.asc[0];
  }

  @Override
  public int compare(Tuple tuple1, Tuple tuple2) {
    for (int i = 0; i < sortKeyIds.length; i++) {
      left = tuple1.get(sortKeyIds[i]);
      right = tuple2.get(sortKeyIds[i]);

      if (left.type() == DatumType.NULL || right.type() == DatumType.NULL) {
        if (!left.equals(right)) {
          if (left.type() == DatumType.NULL) {
            compVal = 1;
          } else if (right.type() == DatumType.NULL) {
            compVal = -1;
          }
          if (nullFirsts[i]) {
            if (compVal != 0) {
              compVal *= -1;
            }
          }
        } else {
          compVal = 0;
        }
      } else {
        if (asc[i]) {
          compVal = left.compareTo(right);
        } else {
          compVal = right.compareTo(left);
        }
      }

      if (compVal < 0 || compVal > 0) {
        return compVal;
      }
    }
    return 0;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TupleComparator) {
      TupleComparator other = (TupleComparator) obj;
      if (sortKeyIds.length != other.sortKeyIds.length) {
        return false;
      }

      for (int i = 0; i < sortKeyIds.length; i++) {
        if (sortKeyIds[i] != other.sortKeyIds[i] ||
            asc[i] != other.asc[i] ||
            nullFirsts[i] != other.nullFirsts[i]) {
          return false;
        }
      }

      return true;
    } else {
      return false;
    }
  }

  @Override
  public void initFromProto() {
  }

  @Override
  public TupleComparatorProto getProto() {
    TupleComparatorProto.Builder builder = TupleComparatorProto.newBuilder();
    SortSpecProto.Builder sortSpecBuilder;

    for (int i = 0; i < sortKeyIds.length; i++) {
      sortSpecBuilder = SortSpecProto.newBuilder();
      sortSpecBuilder.setSortColumnId(sortKeyIds[i]);
      sortSpecBuilder.setAscending(asc[i]);
      sortSpecBuilder.setNullFirst(nullFirsts[i]);
      builder.addSortSpecs(sortSpecBuilder);
    }

    return builder.build();
  }
}