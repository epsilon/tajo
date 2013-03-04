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

package tajo.optimizer.annotated;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;

public class OptimizerUtil {
  /**
   * Find the top logical node matched to type from the given node
   *
   * @param node start node
   * @param type to find
   * @return a found logical node
   */
  public static LogicalOp findTopNode(LogicalPlan node, OpType type) {
    Preconditions.checkNotNull(node);
    Preconditions.checkNotNull(type);

    LogicalOpFinder finder = new LogicalOpFinder(type);
    node.getRootOperator().postOrder(finder);

    if (finder.getFoundNodes().size() == 0) {
      return null;
    }
    return finder.getFoundNodes().get(0);
  }

  private static class LogicalOpFinder implements LogicalOpVisitor {
    private List<LogicalOp> list = new ArrayList<LogicalOp>();
    private final OpType [] tofind;
    private boolean topmost = false;
    private boolean finished = false;

    public LogicalOpFinder(OpType... type) {
      this.tofind = type;
    }

    public LogicalOpFinder(OpType[] type, boolean topmost) {
      this(type);
      this.topmost = topmost;
    }

    @Override
    public void visit(LogicalOp node) {
      if (!finished) {
        for (OpType type : tofind) {
          if (node.getType() == type) {
            list.add(node);
          }
          if (topmost && list.size() > 0) {
            finished = true;
          }
        }
      }
    }

    public List<LogicalOp> getFoundNodes() {
      return list;
    }
  }
}
