/*
 * Copyright (C) 2012-2017 DataStax Inc.
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
package com.datastax.driver.mapping;

import com.datastax.driver.core.CCMTestsSupport;
import com.datastax.driver.core.utils.CassandraVersion;
import com.datastax.driver.mapping.annotations.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("unused")
@CassandraVersion("3.0")
public class MapperMaterializedViewTest extends CCMTestsSupport {

    @Override
    public void onTestContextInitialized() {
        // Example schema taken from: http://www.datastax.com/dev/blog/new-in-cassandra-3-0-materialized-views
        execute(
                "CREATE TABLE scores (user TEXT, game TEXT, year INT, month INT, day INT, score INT, PRIMARY KEY(user, game, year, month, day))",
                "CREATE MATERIALIZED VIEW alltimehigh AS\n"
                        + "       SELECT * FROM scores\n"
                        + "       WHERE game IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL\n"
                        + "       PRIMARY KEY (game, score, user, year, month, day)\n"
                        + "       WITH CLUSTERING ORDER BY (score desc)",
                "CREATE MATERIALIZED VIEW dailyhigh AS\n"
                        + "       SELECT * FROM scores\n"
                        + "       WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL\n"
                        + "       PRIMARY KEY ((game, year, month, day), score, user)\n"
                        + "       WITH CLUSTERING ORDER BY (score DESC)",
                "CREATE MATERIALIZED VIEW monthlyhigh AS\n"
                        + "       SELECT * FROM scores\n"
                        + "       WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND day IS NOT NULL\n"
                        + "       PRIMARY KEY ((game, year, month), score, user, day)\n"
                        + "       WITH CLUSTERING ORDER BY (score DESC)",
                "CREATE MATERIALIZED VIEW filtereduserhigh AS\n"
                        + "       SELECT * FROM scores\n"
                        + "       WHERE user in ('jbellis', 'pcmanus') AND game IS NOT NULL AND score IS NOT NULL AND year is NOT NULL AND day is not NULL and month IS NOT NULL\n"
                        + "       PRIMARY KEY (game, score, user, year, month, day)\n"
                        + "       WITH CLUSTERING ORDER BY (score DESC)",
                "INSERT INTO scores (user, game, year, month, day, score) VALUES ('pcmanus', 'Coup', 2015, 5, 1, 4000)",
                "INSERT INTO scores (user, game, year, month, day, score) VALUES ('jbellis', 'Coup', 2015, 5, 3, 1750)",
                "INSERT INTO scores (user, game, year, month, day, score) VALUES ('yukim', 'Coup', 2015, 5, 3, 2250)",
                "INSERT INTO scores (user, game, year, month, day, score) VALUES ('tjake', 'Coup', 2015, 5, 3, 500)",
                "INSERT INTO scores (user, game, year, month, day, score) VALUES ('iamaleksey', 'Coup', 2015, 6, 1, 2500)",
                "INSERT INTO scores (user, game, year, month, day, score) VALUES ('tjake', 'Coup', 2015, 6, 2, 1000)",
                "INSERT INTO scores (user, game, year, month, day, score) VALUES ('pcmanus', 'Coup', 2015, 6, 2, 2000)",
                "INSERT INTO scores (user, game, year, month, day, score) VALUES ('jmckenzie', 'Coup', 2015, 6, 9, 2700)",
                "INSERT INTO scores (user, game, year, month, day, score) VALUES ('jbellis', 'Coup', 2015, 6, 20, 3500)",
                "INSERT INTO scores (user, game, year, month, day, score) VALUES ('jbellis', 'Checkers', 2015, 6, 20, 1200)",
                "INSERT INTO scores (user, game, year, month, day, score) VALUES ('jbellis', 'Chess', 2015, 6, 21, 3500)",
                "INSERT INTO scores (user, game, year, month, day, score) VALUES ('pcmanus', 'Chess', 2015, 1, 25, 3200)"
        );
    }


    private Mapper<AllTimeHigh> mapper;

    private ScoreAccessor accessor;

    @BeforeMethod(groups = "short")
    public void setUp() {
        MappingManager mappingManager = new MappingManager(session());
        mapper = mappingManager.mapper(AllTimeHigh.class);
        accessor = mappingManager.createAccessor(ScoreAccessor.class);
    }

    /**
     * Validates that a materialized view can be mapped as a regular table.
     *
     * @test_category materialized_view, mapper
     * @jira_ticket JAVA-1258
     */
    @Test(groups = "short")
    public void should_access_mapped_materialized_view() {
        AllTimeHigh allTimeHigh = mapper.get("Coup", 4000, "pcmanus", 2015, 5, 1);
        assertThat(allTimeHigh).isNotNull();
    }

    /**
     * Validates that an accessor properly maps a single entity over a result from a materialized
     * view.
     *
     * @test_category materialized_view, mapper
     */
    @Test(groups = "short")
    public void should_access_single_entity() {
        Score score = accessor.allTimeHigh("Coup");

        assertThat(score).isEqualTo(new Score("pcmanus", "Coup", 2015, 5, 1, 4000));
    }

    /**
     * Validates that an accessor properly maps to a Result from a materialized
     * view.
     *
     * @test_category materialized_view, mapper
     */
    @Test(groups = "short")
    public void should_access_mapped_result() {
        Result<Score> scores = accessor.dailyHigh("Coup", 2015, 6, 2);

        Iterator<Score> iterator = scores.iterator();
        assertThat(iterator.next()).isEqualTo(new Score("pcmanus", "Coup", 2015, 6, 2, 2000));
        assertThat(iterator.next()).isEqualTo(new Score("tjake", "Coup", 2015, 6, 2, 1000));
        assertThat(scores.isExhausted());
    }

    /**
     * Validates that an accessor properly maps to a Result from a materialized
     * view given a range criteria query.
     *
     * @test_category materialized_view, mapper
     */
    @Test(groups = "short")
    public void should_access_mapped_result_range() {
        Result<Score> scores = accessor.monthlyHighRange("Coup", 2015, 6, 2500, 3500);

        Iterator<Score> iterator = scores.iterator();
        assertThat(iterator.next()).isEqualTo(new Score("jbellis", "Coup", 2015, 6, 20, 3500));
        assertThat(iterator.next()).isEqualTo(new Score("jmckenzie", "Coup", 2015, 6, 9, 2700));
        assertThat(iterator.next()).isEqualTo(new Score("iamaleksey", "Coup", 2015, 6, 1, 2500));
        assertThat(scores.isExhausted());
    }

    /**
     * Validates that an accessor properly maps to a Result from a materialized
     * view that is filtered on a primary key.
     *
     * @test_category materialized_view, mapper
     */
    @Test(groups = "short")
    public void should_access_filtered_user_high() {
        Result<Score> scores = accessor.filteredUserHigh("Chess");

        Iterator<Score> iterator = scores.iterator();
        assertThat(iterator.next()).isEqualTo(new Score("jbellis", "Chess", 2015, 6, 21, 3500));
        assertThat(iterator.next()).isEqualTo(new Score("pcmanus", "Chess", 2015, 1, 25, 3200));
        assertThat(scores.isExhausted());
    }

    @Accessor
    public interface ScoreAccessor {
        @Query("select * from alltimehigh where game=:game")
        Score allTimeHigh(@Param("game") String game);

        @Query("select * from monthlyhigh where game=? and year=? and month=? and score >= ? and score <= ?")
        Result<Score> monthlyHighRange(String game, int year, int month, int lowScore, int highScore);

        @Query("select * from dailyhigh where game=? and year=? and month=? and day=?")
        Result<Score> dailyHigh(String game, int year, int month, int day);

        @Query("select * from filtereduserhigh where game=:game")
        Result<Score> filteredUserHigh(String game);
    }

    @Table(name = "scores")
    public static class Score {

        //primary key: user, game, year, month, day

        @PartitionKey
        String user;

        @ClusteringColumn(0)
        String game;

        @ClusteringColumn(1)
        int year;

        @ClusteringColumn(2)
        int month;

        @ClusteringColumn(3)
        int day;

        int score;

        public Score() {
        }

        public Score(String user, String game, int year, int month, int day, int score) {
            this.user = user;
            this.game = game;
            this.year = year;
            this.month = month;
            this.day = day;
            this.score = score;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getGame() {
            return game;
        }

        public void setGame(String game) {
            this.game = game;
        }

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }

        public int getMonth() {
            return month;
        }

        public void setMonth(int month) {
            this.month = month;
        }

        public int getDay() {
            return day;
        }

        public void setDay(int day) {
            this.day = day;
        }

        public int getScore() {
            return score;
        }

        public void setScore(int score) {
            this.score = score;
        }

        @Override
        public String toString() {
            return "Score{" +
                    "user='" + user + '\'' +
                    ", game='" + game + '\'' +
                    ", year=" + year +
                    ", month=" + month +
                    ", day=" + day +
                    ", score=" + score +
                    '}';
        }

        @SuppressWarnings("SimplifiableIfStatement")
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Score))
                return false;

            Score score1 = (Score) o;

            if (year != score1.year)
                return false;
            if (month != score1.month)
                return false;
            if (day != score1.day)
                return false;
            if (score != score1.score)
                return false;
            if (!user.equals(score1.user))
                return false;
            return game.equals(score1.game);
        }

        @Override
        public int hashCode() {
            int result = user.hashCode();
            result = 31 * result + game.hashCode();
            result = 31 * result + year;
            result = 31 * result + month;
            result = 31 * result + day;
            result = 31 * result + score;
            return result;
        }
    }

    @Table(name = "alltimehigh")
    public static class AllTimeHigh {

        //primary key: game, score, user, year, month, day

        @PartitionKey
        String game;

        @ClusteringColumn(0)
        int score;

        @ClusteringColumn(1)
        String user;

        @ClusteringColumn(2)
        int year;

        @ClusteringColumn(3)
        int month;

        @ClusteringColumn(4)
        int day;

        public AllTimeHigh() {
        }

        public AllTimeHigh(String user, String game, int year, int month, int day, int score) {
            this.user = user;
            this.game = game;
            this.year = year;
            this.month = month;
            this.day = day;
            this.score = score;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public String getGame() {
            return game;
        }

        public void setGame(String game) {
            this.game = game;
        }

        public int getYear() {
            return year;
        }

        public void setYear(int year) {
            this.year = year;
        }

        public int getMonth() {
            return month;
        }

        public void setMonth(int month) {
            this.month = month;
        }

        public int getDay() {
            return day;
        }

        public void setDay(int day) {
            this.day = day;
        }

        public int getScore() {
            return score;
        }

        public void setScore(int score) {
            this.score = score;
        }

        @Override
        public String toString() {
            return "AllTimeHigh{" +
                    "user='" + user + '\'' +
                    ", game='" + game + '\'' +
                    ", year=" + year +
                    ", month=" + month +
                    ", day=" + day +
                    ", score=" + score +
                    '}';
        }

        @SuppressWarnings("SimplifiableIfStatement")
        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (!(o instanceof Score))
                return false;

            Score score1 = (Score) o;

            if (year != score1.year)
                return false;
            if (month != score1.month)
                return false;
            if (day != score1.day)
                return false;
            if (score != score1.score)
                return false;
            if (!user.equals(score1.user))
                return false;
            return game.equals(score1.game);
        }

        @Override
        public int hashCode() {
            int result = user.hashCode();
            result = 31 * result + game.hashCode();
            result = 31 * result + year;
            result = 31 * result + month;
            result = 31 * result + day;
            result = 31 * result + score;
            return result;
        }

    }
}
