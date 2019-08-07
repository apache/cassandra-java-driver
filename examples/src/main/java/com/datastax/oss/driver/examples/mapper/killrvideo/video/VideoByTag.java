/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
@CqlName("videos_by_tag")
public class VideoByTag extends VideoBase {
  private String tag;
  private Instant taggedDate;

  public VideoByTag() {}

  public VideoByTag(
      String tag,
      UUID videoid,
      Instant addedDate,
      UUID userid,
      String name,
      String previewImageLocation,
      Instant taggedDate) {
    super(userid, addedDate, videoid, name, previewImageLocation);
    this.tag = tag;
    this.taggedDate = taggedDate;
  }

  @PartitionKey
  public String getTag() {
    return tag;
  }

  public void setTag(String tag) {
    this.tag = tag;
  }

  // Override parent to declare the clustering column:
  @Override
  @ClusteringColumn
  public UUID getVideoid() {
    return super.getVideoid();
  }

  public Instant getTaggedDate() {
    return taggedDate;
  }

  public void setTaggedDate(Instant taggedDate) {
    this.taggedDate = taggedDate;
  }
}
