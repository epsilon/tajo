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

package tajo.planner;

import tajo.algebra.Expr;
import tajo.engine.planner.logical.LogicalNode;

import java.util.PriorityQueue;

public class TajoOptimizer extends AbstractOptimizer{

  @Override
  public LogicalNode optimize(Expr algebra) throws OptimizationException {

    verify(algebra);

    LogicalNode [] joinEnumerated = enumeareJoinOrder();
    PriorityQueue<CostedPlan> heap = new PriorityQueue();

    for (LogicalNode joinOrder : joinEnumerated) {
      LogicalNode step1 = pushdownSelection(joinOrder);
      LogicalNode step2 = pushdownProjection(step1);
      CostedPlan costedPlan = computeCost(step2);
      heap.add(costedPlan);
    }

    return heap.poll().plan;
  }

  public CostedPlan computeCost(LogicalNode optimized) {
    return null;
  }

  public class CostedPlan implements Comparable<CostedPlan> {
    double cost;
    LogicalNode plan;

    public CostedPlan(LogicalNode plan) {
      this.plan = plan;
    }

    public void setCost(double cost) {
      this.cost = cost;
    }

    public double getCost() {
      return this.cost;
    }

    @Override
    public int compareTo(CostedPlan o) {
      double compval = cost - o.cost;
      if (compval < 0) {
        return -1;
      } else if (compval > 0) {
        return 1;
      } else {
        return 0;
      }
    }
  }

  private LogicalNode pushdownProjection(LogicalNode plan) {
    return null;
  }

  private LogicalNode pushdownSelection(LogicalNode plan) {
    return null;
  }

  @Override
  void verify(Expr algebra) throws OptimizationException {
  }

  LogicalNode[] enumeareJoinOrder() {
    return new LogicalNode[0];  //To change body of implemented methods use File | Settings | File Templates.
  }
}
