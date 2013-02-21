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

import tajo.catalog.SortSpec;
import tajo.storage.TupleRange;

public class RangePartition extends Repartition {
  private SortSpec[] sortSpecs;
  private TupleRange [] ranges;

  public RangePartition(ChannelType type,
                        SortSpec[] sortSpecs,
                        int partitionNum) {
    super(PartitionType.RANGE, type, partitionNum);
    this.sortSpecs = sortSpecs;
  }

  public boolean hasRanges() {
    return this.ranges != null;
  }

  public void setRanges(TupleRange [] ranges) {
    this.ranges = ranges;
  }

  public SortSpec [] getSortSpecs() {
    return this.sortSpecs;
  }
}
