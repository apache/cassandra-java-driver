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

import java.time.Instant;
import java.util.UUID;

/** Information common to all the denormalized video tables. */
public abstract class VideoBase {
  private UUID userid;
  private Instant addedDate;
  private UUID videoid;
  private String name;
  private String previewImageLocation;

  protected VideoBase() {}

  protected VideoBase(
      UUID userid, Instant addedDate, UUID videoid, String name, String previewImageLocation) {
    this.userid = userid;
    this.addedDate = addedDate;
    this.videoid = videoid;
    this.name = name;
    this.previewImageLocation = previewImageLocation;
  }

  public UUID getUserid() {
    return userid;
  }

  public void setUserid(UUID userid) {
    this.userid = userid;
  }

  public Instant getAddedDate() {
    return addedDate;
  }

  public void setAddedDate(Instant addedDate) {
    this.addedDate = addedDate;
  }

  public UUID getVideoid() {
    return videoid;
  }

  public void setVideoid(UUID videoid) {
    this.videoid = videoid;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPreviewImageLocation() {
    return previewImageLocation;
  }

  public void setPreviewImageLocation(String previewImageLocation) {
    this.previewImageLocation = previewImageLocation;
  }
}
