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

package tajo.util;

import com.google.gson.Gson;
import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.SortSpec;
import tajo.datum.Datum;
import tajo.engine.eval.ConstEval;
import tajo.engine.eval.EvalNode;
import tajo.engine.eval.EvalNode.Type;
import tajo.engine.eval.EvalNodeVisitor;
import tajo.engine.eval.FieldEval;
import tajo.engine.json.GsonCreator;
import tajo.engine.planner.logical.IndexScanNode;
import tajo.engine.planner.logical.ScanNode;
import tajo.storage.Fragment;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

public class IndexUtil {
  public static String getIndexNameOfFrag(Fragment fragment, SortSpec[] keys) {
    StringBuilder builder = new StringBuilder(); 
    builder.append(fragment.getPath().getName() + "_");
    builder.append(fragment.getStartOffset() + "_" + fragment.getLength() + "_");
    for(int i = 0 ; i < keys.length ; i ++) {
      builder.append(keys[i].getKey().getColumnName()+"_");
    }
    builder.append("_index");
    return builder.toString();
       
  }
  
  public static String getIndexName (String indexName , SortSpec[] keys) {
    StringBuilder builder = new StringBuilder();
    builder.append(indexName + "_");
    for(int i = 0 ; i < keys.length ; i ++) {
      builder.append(keys[i].getKey().getColumnName() + "_");
    }
    return builder.toString();
  }
  
  public static IndexScanNode indexEval( ScanNode scanNode, 
      Iterator<Entry<String, String>> iter ) {
   
    EvalNode qual = scanNode.getQual();
    Gson gson = GsonCreator.getInstance();
    
    FieldAndValueFinder nodeFinder = new FieldAndValueFinder();
    qual.preOrder(nodeFinder);
    LinkedList<EvalNode> nodeList = nodeFinder.getNodeList();
    
    int maxSize = Integer.MIN_VALUE;
    SortSpec[] maxIndex = null;
    
    String json;
    while(iter.hasNext()) {
      Entry<String , String> entry = iter.next();
      json = entry.getValue();
      SortSpec[] sortKey = gson.fromJson(json, SortSpec[].class);
      if(sortKey.length > nodeList.size()) {
        /* If the number of the sort key is greater than where condition, 
         * this index cannot be used
         * */
        continue; 
      } else {
        boolean[] equal = new boolean[sortKey.length];
        for(int i = 0 ; i < sortKey.length ; i ++) {
          for(int j = 0 ; j < nodeList.size() ; j ++) {
            Column col = ((FieldEval)(nodeList.get(j).getLeftExpr())).getColumnRef();
            if(col.equals(sortKey[i].getKey())) {
              equal[i] = true;
            }
          }
        }
        boolean chk = true;
        for(int i = 0 ; i < equal.length ; i ++) {
          chk = chk && equal[i];
        }
        if(chk) {
          if(maxSize < sortKey.length) {
            maxSize = sortKey.length;
            maxIndex = sortKey;
          }
        }
      }
    }
    if(maxIndex == null) {
      return null;
    } else {
      Schema keySchema = new Schema();
      for(int i = 0 ; i < maxIndex.length ; i ++ ) {
        keySchema.addColumn(maxIndex[i].getKey());
      }
      Datum[] datum = new Datum[nodeList.size()];
      for(int i = 0 ; i < nodeList.size() ; i ++ ) {
        datum[i] = ((ConstEval)(nodeList.get(i).getRightExpr())).getValue();
      }
      
      return new IndexScanNode(scanNode, keySchema , datum , maxIndex);
    }

  }
  
  
  private static class FieldAndValueFinder implements EvalNodeVisitor {
    private LinkedList<EvalNode> nodeList = new LinkedList<EvalNode>();
    
    public LinkedList<EvalNode> getNodeList () {
      return this.nodeList;
    }
    
    @Override
    public void visit(EvalNode node) {
      switch(node.getType()) {
      case AND:
        break;
      case EQUAL:
        if( node.getLeftExpr().getType() == Type.FIELD 
          && node.getRightExpr().getType() == Type.CONST ) {
          nodeList.add(node);
        }
        break;
      case IS:
        if( node.getLeftExpr().getType() == Type.FIELD 
          && node.getRightExpr().getType() == Type.CONST) {
          nodeList.add(node);
        }
      }
    }
  }
}
