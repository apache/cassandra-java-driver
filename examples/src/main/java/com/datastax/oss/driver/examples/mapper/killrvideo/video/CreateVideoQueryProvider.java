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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.api.mapper.MapperContext;
import com.datastax.oss.driver.api.mapper.entity.EntityHelper;
import com.datastax.oss.driver.api.mapper.entity.saving.NullSavingStrategy;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Provides the implementation of {@link VideoDao#create}.
 *
 * <p>Package-private visibility is sufficient, this will be called only from the generated DAO
 * implementation.
 */
class CreateVideoQueryProvider {

  private final CqlSession session;
  private final EntityHelper<Video> videoHelper;
  private final EntityHelper<UserVideo> userVideoHelper;
  private final EntityHelper<LatestVideo> latestVideoHelper;
  private final EntityHelper<VideoByTag> videoByTagHelper;
  private final PreparedStatement preparedInsertVideo;
  private final PreparedStatement preparedInsertUserVideo;
  private final PreparedStatement preparedInsertLatestVideo;
  private final PreparedStatement preparedInsertVideoByTag;

  CreateVideoQueryProvider(
      MapperContext context,
      EntityHelper<Video> videoHelper,
      EntityHelper<UserVideo> userVideoHelper,
      EntityHelper<LatestVideo> latestVideoHelper,
      EntityHelper<VideoByTag> videoByTagHelper) {
    this.session = context.getSession();
    this.videoHelper = videoHelper;
    this.userVideoHelper = userVideoHelper;
    this.latestVideoHelper = latestVideoHelper;
    this.videoByTagHelper = videoByTagHelper;

    this.preparedInsertVideo = prepareInsert(session, videoHelper);
    this.preparedInsertUserVideo = prepareInsert(session, userVideoHelper);
    this.preparedInsertLatestVideo = prepareInsert(session, latestVideoHelper);
    this.preparedInsertVideoByTag = prepareInsert(session, videoByTagHelper);
  }

  void create(Video video) {
    if (video.getVideoid() == null) {
      video.setVideoid(Uuids.random());
    }
    if (video.getAddedDate() == null) {
      video.setAddedDate(Instant.now());
    }

    BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.LOGGED);
    batch.addStatement(bind(preparedInsertVideo, video, videoHelper));
    batch.addStatement(bind(preparedInsertUserVideo, toUserVideo(video), userVideoHelper));
    batch.addStatement(bind(preparedInsertLatestVideo, toLatestVideo(video), latestVideoHelper));
    if (video.getTags() != null) {
      for (String tag : video.getTags()) {
        batch.addStatement(
            bind(preparedInsertVideoByTag, toVideoByTag(video, tag), videoByTagHelper));
      }
    }
    session.execute(batch.build());
  }

  private static <T> PreparedStatement prepareInsert(
      CqlSession session, EntityHelper<T> entityHelper) {
    return session.prepare(entityHelper.insert().asCql());
  }

  private static <T> BoundStatement bind(
      PreparedStatement preparedStatement, T entity, EntityHelper<T> entityHelper) {
    BoundStatementBuilder boundStatement = preparedStatement.boundStatementBuilder();
    entityHelper.set(entity, boundStatement, NullSavingStrategy.DO_NOT_SET, false);
    return boundStatement.build();
  }

  private static UserVideo toUserVideo(Video video) {
    return new UserVideo(
        video.getUserid(),
        video.getAddedDate(),
        video.getVideoid(),
        video.getName(),
        video.getPreviewImageLocation());
  }

  private static LatestVideo toLatestVideo(Video video) {
    return new LatestVideo(
        DAY_FORMATTER.format(video.getAddedDate()),
        video.getAddedDate(),
        video.getVideoid(),
        video.getUserid(),
        video.getName(),
        video.getPreviewImageLocation());
  }

  private static final DateTimeFormatter DAY_FORMATTER =
      DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

  private static VideoByTag toVideoByTag(Video video, String tag) {
    return new VideoByTag(
        tag,
        video.getVideoid(),
        video.getAddedDate(),
        video.getUserid(),
        video.getName(),
        video.getPreviewImageLocation(),
        video.getAddedDate());
  }
}
