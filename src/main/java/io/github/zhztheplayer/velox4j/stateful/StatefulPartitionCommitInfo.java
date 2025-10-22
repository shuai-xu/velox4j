/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package io.github.zhztheplayer.velox4j.stateful;

public class StatefulPartitionCommitInfo extends StatefulElement {

  private long checkpointId;
  private int taskId;
  private int numberOfTasks;
  private String[] partitions;

  public StatefulPartitionCommitInfo(
      String nodeId,
      long checkpointId,
      int taskId,
      int numberOfTasks,
      String[] partitions) {
    super(nodeId);
    this.checkpointId = checkpointId;
    this.taskId = taskId;
    this.numberOfTasks = numberOfTasks;
    this.partitions = partitions;
  }

  public long getCheckpointId() {
    return checkpointId;
  }

  public int getTaskId() {
    return taskId;
  }

  public int getNumberOfTasks() {
    return numberOfTasks;
  }

  public String[] getPartitions() {
    return partitions;
  }

  @Override
  public boolean isWatermark() {
    return false;
  }

  @Override
  public boolean isRecord() {
    return false;
  }

  @Override
  public boolean isPartitionInfo() {
    return true;
  }
}
