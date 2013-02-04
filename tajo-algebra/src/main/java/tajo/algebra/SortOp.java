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

package tajo.algebra;

public class SortOp extends UnaryOp {
  private SortSpec [] sortSpecs;

  public SortOp() {
    super(OperatorType.Sort);
  }

  public void setSortSpecs(SortSpec [] sortSpecs) {
    this.sortSpecs = sortSpecs;
  }

  public SortSpec [] getSortSpecs() {
    return this.sortSpecs;
  }

  @Override
  public String toJson() {
    return JsonHelper.toJson(this);
  }

  public static class SortSpec {
    private String sortKey;
    private boolean ascending = true;
    private boolean nullFirst = false;

    public SortSpec(final String sortKey) {
      this.sortKey = sortKey;
    }

    /**
     *
     * @param sortKey a column to sort
     * @param asc true if the sort order is ascending order
     * @param nullFirst
     * Otherwise, it should be false.
     */
    public SortSpec(final String sortKey, final boolean asc,
                    final boolean nullFirst) {
      this(sortKey);
      this.ascending = asc;
      this.nullFirst = nullFirst;
    }

    public final boolean isAscending() {
      return this.ascending;
    }

    public final void setDescOrder() {
      this.ascending = false;
    }

    public final boolean isNullFirst() {
      return this.nullFirst;
    }

    public final void setNullFirst() {
      this.nullFirst = true;
    }

    public final String getSortKey() {
      return this.sortKey;
    }
  }
}
