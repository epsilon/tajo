/*
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

/**
 * 
 */
package tajo.optimizer.annotated;

/**
 * @author Hyunsik Choi
 *
 */
public enum OpType {
  CREATE_INDEX,
  CREATE_TABLE,
	EXCEPT,
	EXPRS,
	GROUP_BY,
	INSERT_INTO,
	INTERSECT,
  LIMIT,
	JOIN(JoinOp.class),
  Projection,
	RECEIVE,
	RENAME,
  Relation,
  RelationList,
  Selection,
	SEND,
	SET_DIFF, 
	SET_UNION,
  SET_INTERSECT,
	SORT,
	STORE,
	UNION,
  ScalarSubQuery,
  TableSubQuery;

  private Class<? extends LogicalOp> clazz;
  OpType() {
    this.clazz = LogicalOp.class;
  }

  OpType(Class<? extends LogicalOp> clazz) {
    this.clazz = clazz;
  }

  public Class<? extends LogicalOp> getOpClass() {
    return this.clazz;
  }
}
