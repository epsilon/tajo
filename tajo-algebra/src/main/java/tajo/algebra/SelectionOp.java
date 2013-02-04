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

package tajo.algebra;

import com.google.gson.*;
import tajo.engine.eval.EvalNode;

public class SelectionOp extends UnaryOp implements Cloneable {
	private String relation;
  private EvalNode qual;

  public SelectionOp(String relation) {
    super(OperatorType.SELECTION);
    this.relation = relation;
  }

  public String getRelation() {
    return this.relation;
  }

  public boolean hasQual() {
    return qual != null;
  }

	public EvalNode getQual() {
		return this.qual;
	}
  
  public String toString() {
    Gson gson = new Gson();
    return gson.toJson(this);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SelectionOp) {
      SelectionOp other = (SelectionOp) obj;
      return super.equals(other) 
          && this.qual.equals(other.qual)
          && subExpr.equals(other.subExpr);
    } else {
      return false;
    }
  }
}
