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

package tajo.optimizer;

import com.google.common.base.Preconditions;
import tajo.engine.eval.EvalNode;
import tajo.engine.planner.PlannerUtil;
import tajo.optimizer.annotated.*;
import tajo.optimizer.annotated.join.JoinGraph;

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
  public static LogicalOp findTopNodeFromRootBlock(LogicalPlan node,OpType type) {
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
    public boolean accept(LogicalOp node) {
      return node.getType() != OpType.TableSubQuery;
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

  public static JoinGraph createJoinGraph(EvalNode [] cnf) {
    JoinGraph joinGraph = new JoinGraph();
    for (EvalNode expr : cnf) {
      if (PlannerUtil.isJoinQual(expr)) {
        joinGraph.addJoin(expr);
      }
    }

    return joinGraph;
  }

  public static String buildJoinOrderToString(JoinOp joinNode) {
    StringBuilder sb = new StringBuilder();
    traverseJoinNode(sb, joinNode);
    return sb.toString();
  }

  public static void traverseJoinNode(StringBuilder stringBuilder, LogicalOp node) {
    if (node.getType() == OpType.JOIN) {
      JoinOp join = (JoinOp) node;
      stringBuilder.append("(");
      traverseJoinNode(stringBuilder, join.getLeftOp());
      stringBuilder.append(",");
      traverseJoinNode(stringBuilder, join.getRightOp());
      stringBuilder.append(")");
    } else if (node.getType() == OpType.Relation) {
      RelationOp scan = (RelationOp) node;
      stringBuilder.append(scan.getCanonicalName());
    }
  }
}