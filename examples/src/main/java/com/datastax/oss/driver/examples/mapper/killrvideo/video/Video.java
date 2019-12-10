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

import com.datastax.oss.driver.api.mapper.annotations.CqlName;
import com.datastax.oss.driver.api.mapper.annotations.Entity;
import com.datastax.oss.driver.api.mapper.annotations.PartitionKey;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

@Entity
@CqlName("videos")
public class Video extends VideoBase {

  private String description;
  private String location;
  private int locationType;
  private Set<String> tags;

  public Video() {}

  public Video(
      UUID videoid,
      UUID userid,
      String name,
      String description,
      String location,
      int locationType,
      String previewImageLocation,
      Set<String> tags,
      Instant addedDate) {
    super(userid, addedDate, videoid, name, previewImageLocation);
    this.description = description;
    this.location = location;
    this.locationType = locationType;
    this.tags = tags;
  }

  // Override parent getter to declare the partition key:
  @Override
  @PartitionKey
  public UUID getVideoid() {
    return super.getVideoid();
  }

  public String getDescription() {
    return description;
  }

  public void setDescription(String description) {
    this.description = description;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public int getLocationType() {
    return locationType;
  }

  public void setLocationType(int locationType) {
    this.locationType = locationType;
  }

  public Set<String> getTags() {
    return tags;
  }

  public void setTags(Set<String> tags) {
    this.tags = tags;
  }
}
