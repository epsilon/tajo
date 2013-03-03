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
import tajo.optimizer.annotated.LogicalPlan;

import java.util.TreeMap;

public class QueryRewriteEngine {

  private CatalogService catalog;
  /**
   * key - priority, value - rule
   */
  private TreeMap<Integer, RewriteRule> rules = new TreeMap<Integer, RewriteRule>();

  public QueryRewriteEngine(CatalogService catalog) {
    this.catalog = catalog;
  }

  public LogicalPlan rewrite(LogicalPlan plan) {

    LogicalPlan rewritten = plan;
    // apply rewrite rule in an ascending order of priorities.
    for (RewriteRule rule : rules.values()) {
      if (rule.isFeasible(rewritten))
      rewritten = rule.apply(plan);
    }
    return rewritten;
  }
}
