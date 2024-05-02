/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.examples.mapper.killrvideo.video;

import com.datastax.oss.driver.api.mapper.annotations.ClusteringColumn;
import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import java.time.Instant;
import java.util.UUID;

@Entity
@CqlName("latest_videos")
public class LatestVideo extends VideoBase {
  @PartitionKey private String yyyymmdd;

  public LatestVideo() {}

  public LatestVideo(
      String yyyymmdd,
      Instant addedDate,
      UUID videoid,
      UUID userid,
      String name,
      String previewImageLocation) {
    super(userid, addedDate, videoid, name, previewImageLocation);
    this.yyyymmdd = yyyymmdd;
  }

  public String getYyyymmdd() {
    return yyyymmdd;
  }

  public void setYyyymmdd(String yyyymmdd) {
    this.yyyymmdd = yyyymmdd;
  }

  // Override parent getters to declare clustering columns:
  @Override
  @ClusteringColumn(0)
  public Instant getAddedDate() {
    return super.getAddedDate();
  }

  @Override
  @ClusteringColumn(1)
  public UUID getVideoid() {
    return super.getVideoid();
  }
}
