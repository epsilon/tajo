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

import com.google.gson.annotations.Expose;

public abstract class BinaryOp extends LogicalOp implements Cloneable {
	@Expose LogicalOp outer = null;
	@Expose LogicalOp inner = null;

	public BinaryOp(int id, OpType type) {
		super(id, type);
	}

	public LogicalOp getLeftOp() {
		return this.outer;
	}

	public void setLeftOp(LogicalOp op) {
		this.outer = op;
	}

	public LogicalOp getRightOp() {
		return this.inner;
	}

	public void setRightOp(LogicalOp op) {
		this.inner = op;
	}

	@Override
  public Object clone() throws CloneNotSupportedException {
	  BinaryOp binNode = (BinaryOp) super.clone();
	  binNode.outer = (LogicalOp) outer.clone();
	  binNode.inner = (LogicalOp) inner.clone();

	  return binNode;
	}

  @Override
  public void preOrder(LogicalOpVisitor visitor) {
    visitor.visit(this);
    outer.preOrder(visitor);
    inner.preOrder(visitor);
  }

  @Override
  public void postOrder(LogicalOpVisitor visitor) {
    outer.postOrder(visitor);
    inner.postOrder(visitor);
    visitor.visit(this);
  }
}
