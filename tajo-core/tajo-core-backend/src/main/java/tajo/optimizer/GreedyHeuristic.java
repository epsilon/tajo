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

import tajo.catalog.CatalogService;
import tajo.optimizer.annotated.*;

import java.util.*;

/**
 * A greedy heuristic to find a left-deep join tree
 */
public class GreedyHeuristic implements JoinOrderAlgorithm {

  private CatalogService catalog;
  public GreedyHeuristic(CatalogService catalog) {
    this.catalog = catalog;
  }

  @Override
  public LogicalPlan findBestOrder(LogicalPlan plan) {
    SelectionOp selection = (SelectionOp) OptimizerUtil.findTopNode(plan, OpType.Selection);

    RelationListOp relationList = (RelationListOp) OptimizerUtil.findTopNode(plan,
        OpType.RelationList);

    // Build a join graph
    JoinGraph joinGraph = OptimizerUtil.createJoinGraph(selection.getQual());

    // Build a map (a relation name -> a relation object)
    // initialize a relation set
    HashMap<String, RelationOp> relationMap = new HashMap<String, RelationOp>();
    Set<String> relationSet = new HashSet<String>();
    for (LogicalOp op : relationList.getRelations()) {
      if (op instanceof TableSubQueryOp) {
        TableSubQueryOp tableSubQuery = (TableSubQueryOp) op;
        relationMap.put(tableSubQuery.getName(), tableSubQuery);
        relationSet.add(tableSubQuery.getName());
      } else if (op instanceof RelationOp) {
        RelationOp rel = (RelationOp) op;
        relationMap.put(rel.getRelationId(), rel);
        relationSet.add(rel.getRelationId());
      }
    }

    // A set of joined relations
    Set<String> joinedRelNames = new HashSet<String>();

    // Get candidates from all relations to be joined
    List<RelationOp> candidates = getSortedCandidates(relationMap, relationSet);
    JoinOp join = plan.createLogicalOp(JoinOp.class);

    RelationOp first = candidates.get(0); // Get the first candidate relation
    relationSet.remove(first.getRelationId()); // Remove the first candidate relation

    join.setOuter(first); // Set the first candidate to a outer relation of the first join
    // Add the first candidate to the set of joined relations
    joinedRelNames.add(first.getRelationId());

    JoinOp prev = join;
    while(true) {
      // Get a set of relations that can be joined to the composite relation.
      Set<String> candidateName = new HashSet<String>();
      for (String joinedRelation : joinedRelNames) {
        Collection<Edge> edges = joinGraph.getEdges(joinedRelation);
        for (Edge edge : edges) {
          if (!joinedRelNames.contains(edge.getTarget())) {
            candidateName.add(edge.getTarget());
          }
        }
      }

      // Get a sorted candidates from the set of relations that can be joined
      candidates = getSortedCandidates(relationMap, candidateName);

      // Get a candidate relation such that the candidate incurs the smallest intermediate and is
      // not join to any relation yet.
      RelationOp chosen = null;
      for (int i = 0; i < candidates.size(); i++) {
        chosen = candidates.get(i);
        if (relationSet.contains(chosen.getRelationId())) {
          break;
        }
      }

      // Set the candidate to a inner relation and remove from the relation set.
      prev.setInner(chosen);
      joinedRelNames.add(chosen.getRelationId());
      relationSet.remove(chosen.getRelationId());

      // If the relation set is empty, stop this loop.
      if (relationSet.isEmpty()) {
        break;
      } else {
        // Otherwise, the least join becomes an outer one for the next join
        join = plan.createLogicalOp(JoinOp.class);
        join.setOuter(prev);
        prev = join;
      }
    }

    TajoOptimizer.printJoinOrder(prev);

    return null;
  }

  private List<RelationOp> getSortedCandidates(Map<String, RelationOp> map, Set<String> relationSet) {
    List<RelationOp> relations = new ArrayList<RelationOp>();
    for (String name : relationSet) {
      relations.add(map.get(name));
    }
    Collections.sort(relations, new RelationComparator());
    return relations;
  }

  private long getCost(RelationOp op) {
    if (op instanceof RelationOp) {
      RelationOp rel = op;
      return rel.getMeta().getStat().getNumBytes();
    } else if (op instanceof  TableSubQueryOp) {
      throw new IllegalStateException();
    } else {
     throw new IllegalStateException();
    }
  }

  class RelationComparator implements Comparator<RelationOp> {
    @Override
    public int compare(RelationOp o1, RelationOp o2) {
      if (getCost(o1) < getCost(o2)) {
        return -1;
      } else if (getCost(o1) > getCost(o2)) {
        return 1;
      } else {
        return 0;
      }
    }
  }
}
