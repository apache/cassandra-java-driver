// User credentials, keyed by email address so we can authenticate
CREATE TABLE IF NOT EXISTS user_credentials (
    email text,
    password text,
    userid uuid,
    PRIMARY KEY (email)
);

// Users keyed by id
CREATE TABLE IF NOT EXISTS users (
    userid uuid,
    firstname text,
    lastname text,
    email text,
    created_date timestamp,
    PRIMARY KEY (userid)
);

// Videos by id
CREATE TABLE IF NOT EXISTS videos (
    videoid uuid,
    userid uuid,
    name text,
    description text,
    location text,
    location_type int,
    preview_image_location text,
    tags set<text>,
    added_date timestamp,
    PRIMARY KEY (videoid)
);

// One-to-many from user point of view (lookup table)
CREATE TABLE IF NOT EXISTS user_videos (
    userid uuid,
    added_date timestamp,
    videoid uuid,
    name text,
    preview_image_location text,
    PRIMARY KEY (userid, added_date, videoid)
) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);

// Track latest videos, grouped by day (if we ever develop a bad hotspot from the daily grouping here, we could mitigate by
// splitting the row using an arbitrary group number, making the partition key (yyyymmdd, group_number))
CREATE TABLE IF NOT EXISTS latest_videos (
    yyyymmdd text,
    added_date timestamp,
    videoid uuid,
    userid uuid,
    name text,
    preview_image_location text,
    PRIMARY KEY (yyyymmdd, added_date, videoid)
) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);

// Video ratings (counter table)
CREATE TABLE IF NOT EXISTS video_ratings (
    videoid uuid,
    rating_counter counter,
    rating_total counter,
    PRIMARY KEY (videoid)
);

// Video ratings by user (to try and mitigate voting multiple times)
CREATE TABLE IF NOT EXISTS video_ratings_by_user (
    videoid uuid,
    userid uuid,
    rating int,
    PRIMARY KEY (videoid, userid)
);

// Records the number of views/playbacks of a video
CREATE TABLE IF NOT EXISTS video_playback_stats (
    videoid uuid,
    views counter,
    PRIMARY KEY (videoid)
);

// Recommendations by user (powered by Spark), with the newest videos added to the site always first
CREATE TABLE IF NOT EXISTS video_recommendations ( 
    userid uuid,
    added_date timestamp,
    videoid uuid,
    rating float,
    authorid uuid,
    name text,
    preview_image_location text,
    PRIMARY KEY(userid, added_date, videoid)
) WITH CLUSTERING ORDER BY (added_date DESC, videoid ASC);

// Recommendations by video (powered by Spark)
CREATE TABLE IF NOT EXISTS video_recommendations_by_video (
    videoid uuid,
    userid uuid,
    rating float,
    added_date timestamp STATIC,
    authorid uuid STATIC,
    name text STATIC,
    preview_image_location text STATIC,
    PRIMARY KEY(videoid, userid)
);

// Index for tag keywords
CREATE TABLE IF NOT EXISTS videos_by_tag (
    tag text,
    videoid uuid,
    added_date timestamp,
    userid uuid,
    name text,
    preview_image_location text,
    tagged_date timestamp,
    PRIMARY KEY (tag, videoid)
);

// Index for tags by first letter in the tag
CREATE TABLE IF NOT EXISTS tags_by_letter (
    first_letter text,
    tag text,
    PRIMARY KEY (first_letter, tag)
);

// Comments for a given video
CREATE TABLE IF NOT EXISTS comments_by_video (
    videoid uuid,
    commentid timeuuid,
    userid uuid,
    comment text,
    PRIMARY KEY (videoid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);

// Comments for a given user
CREATE TABLE IF NOT EXISTS comments_by_user (
    userid uuid,
    commentid timeuuid,
    videoid uuid,
    comment text,
    PRIMARY KEY (userid, commentid)
) WITH CLUSTERING ORDER BY (commentid DESC);
