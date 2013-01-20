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

import tajo.engine.eval.EvalNode;

/**
 * 
 * @author Hyunsik Choi
 *
 */
public class SelectionExpr extends UnaryExpr implements Cloneable {
	private final Relation rel;
  private EvalNode qual;

  public SelectionExpr(Relation rel) {
    super(ExprType.SELECTION);
    this.rel = rel;
  }

	public SelectionExpr(Relation rel, EvalNode qual) {
		this(rel);
		this.qual = qual;
	}

  public boolean hasQual() {
    return qual != null;
  }

	public EvalNode getQual() {
		return this.qual;
	}
  
  public String toString() {
    StringBuilder sb = new StringBuilder(type.getName());
    sb.append("(");
    sb.append(rel.getName());
    if (hasQual()) {
      sb.append(",");
      sb.append(qual);
    }
    sb.append(")");
    return sb.toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SelectionExpr) {
      SelectionExpr other = (SelectionExpr) obj;
      return super.equals(other) 
          && this.qual.equals(other.qual)
          && subExpr.equals(other.subExpr);
    } else {
      return false;
    }
  }
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    SelectionExpr selNode = (SelectionExpr) super.clone();
    selNode.qual = (EvalNode) this.qual.clone();
    
    return selNode;
  }
}
