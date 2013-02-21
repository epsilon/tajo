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

public abstract class Repartition {
  public static enum PartitionType {
    HASH, // hash repartition by hash columns
    RANGE, // range repartition by columns
    ROUND_ROBIN
  }

  public static enum ChannelType {
    PULL, // Intermediate data are materialized into disk and pulled.
    PUSH // Intermediate data will be sent via TCP channel
  }

  protected PartitionType partitionType;
  protected ChannelType channelType;
  protected int partitionNum;

  public Repartition(PartitionType partitionType, ChannelType channelType, int partitionNum) {
    this.partitionType = partitionType;
    this.channelType = channelType;
    this.partitionNum = partitionNum;
  }

  public PartitionType getPartitionType() {
    return this.partitionType;
  }

  public ChannelType getChannelType() {
    return this.channelType;
  }

  public int getPartitionNum() {
    return this.partitionNum;
  }

  public void setPartitionNum(int partNum) {
    this.partitionNum = partNum;
  }
}
