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

package tajo.optimizer.annotated.join;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import tajo.catalog.Column;
import tajo.engine.eval.EvalNode;
import tajo.engine.eval.EvalTreeUtil;
import tajo.optimizer.DotGraphObject;

import java.util.*;

public class JoinGraph implements DotGraphObject {
  private Map<String,List<JoinCondEdge>> graph = new HashMap<String, List<JoinCondEdge>>();

  public void addJoin(EvalNode node) {
    List<Column> left = EvalTreeUtil.findAllColumnRefs(node.getLeftExpr());
    List<Column> right = EvalTreeUtil.findAllColumnRefs(node.getRightExpr());

    String ltbName = left.get(0).getTableName();
    String rtbName = right.get(0).getTableName();

    JoinCondEdge l2r = new JoinCondEdge(ltbName, rtbName, node);
    JoinCondEdge r2l = new JoinCondEdge(rtbName, ltbName, node);
    List<JoinCondEdge> edges;
    if (graph.containsKey(ltbName)) {
      edges = graph.get(ltbName);
    } else {
      edges = Lists.newArrayList();
    }
    edges.add(l2r);
    graph.put(ltbName, edges);

    if (graph.containsKey(rtbName)) {
      edges = graph.get(rtbName);
    } else {
      edges = Lists.newArrayList();
    }
    edges.add(r2l);
    graph.put(rtbName, edges);
  }

  public int degree(String tableName) {
    return this.graph.get(tableName).size();
  }

  public Collection<String> getTables() {
    return Collections.unmodifiableCollection(graph.keySet());
  }

  public Collection<JoinCondEdge> getEdges(String tableName) {
    return Collections.unmodifiableCollection(graph.get(tableName));
  }

  public Collection<JoinCondEdge> getUndirectEdges() {
    Set<JoinCondEdge> edges = new HashSet<JoinCondEdge>();

    for (JoinCondEdge edge : getAllEdges()) {
      if (edge.getSrc().compareToIgnoreCase(edge.getTarget()) > 0) {
        edges.add(new JoinCondEdge(edge.getTarget(), edge.getSrc(), edge.getJoinQual()));
      }
    }
    return edges;
  }

  public Collection<JoinCondEdge> getAllEdges() {
    List<JoinCondEdge> edges = Lists.newArrayList();
    for (List<JoinCondEdge> edgeList : graph.values()) {
      edges.addAll(edgeList);
    }
    return Collections.unmodifiableCollection(edges);
  }

  public int getTableNum() {
    return this.graph.size();
  }

  @Override
  public String getDotGraph() {
    Collection<JoinCondEdge> undirectedEdges = getUndirectEdges();

    StringBuilder sb = new StringBuilder();
    sb.append("graph G {\n");
    for (JoinCondEdge edge : undirectedEdges) {
        sb.append("  ").append(edge.getSrc());
        sb.append(" -- ").append(edge.getTarget())
            .append(" [label=\"").append(edge.getJoinQual().toString()).append("\"];");
        sb.append("\n");
    }
    sb.append("}");
    return sb.toString();
  }
}
