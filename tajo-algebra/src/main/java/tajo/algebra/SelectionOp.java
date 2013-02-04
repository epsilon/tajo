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

public class SelectionOp extends UnaryOp implements JsonSerializable {
	private Relation relation;
  private Expr qual;

  public SelectionOp(Relation relation) {
    super(OperatorType.Selection);
    this.relation = relation;
  }

  public Relation getRelation() {
    return this.relation;
  }

  public boolean hasQual() {
    return qual != null;
  }

  public void setQual(Expr expr) {
    this.qual = expr;
  }

	public Expr getQual() {
		return this.qual;
	}

  
  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SelectionOp) {
      SelectionOp other = (SelectionOp) obj;
      return super.equals(other) 
          && this.qual.equals(other.qual);
    } else {
      return false;
    }
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }
}
