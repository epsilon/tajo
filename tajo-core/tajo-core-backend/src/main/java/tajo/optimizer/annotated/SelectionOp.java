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

package tajo.optimizer.annotated;

import com.google.gson.annotations.Expose;
import tajo.engine.eval.EvalNode;
import tajo.engine.json.GsonCreator;
import tajo.engine.planner.logical.ExprType;
import tajo.engine.planner.logical.LogicalNode;
import tajo.engine.planner.logical.UnaryNode;

public class SelectionOp extends UnaryOp implements Cloneable {
	@Expose	private EvalNode qual;

	public SelectionOp(Integer id) {
		super(id, OpType.SELECTION);
	}

  public void init(EvalNode qual) {
    setQual(qual);
  }

	public EvalNode getQual() {
		return this.qual;
	}

	public void setQual(EvalNode qual) {
		this.qual = qual;
	}
  
  public String toString() {
    return null;
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SelectionOp) {
      SelectionOp other = (SelectionOp) obj;
      return super.equals(other) 
          && this.qual.equals(other.qual)
          && child.equals(other.child);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    SelectionOp selNode = (SelectionOp) super.clone();
    selNode.qual = (EvalNode) this.qual.clone();
    
    return selNode;
  }
}
