package com.datastax.driver.mapping

import com.datastax.driver.core.CCMTestsSupport
import com.datastax.driver.mapping.annotations.Column
import com.datastax.driver.mapping.annotations.PartitionKey
import com.datastax.driver.mapping.annotations.Table
import org.testng.annotations.Test

import static org.assertj.core.api.Assertions.assertThat

/**
 * A simple test to check that mapping a Groovy class works as expected.
 *
 * @jira_ticket JAVA-1279
 */
public class MapperGroovyTest extends CCMTestsSupport {

    @Table(name = "users")
    static class User {

        @PartitionKey
        @Column(name = "user_id")
        def UUID userId

        def String name

    }

    @Override
    public void onTestContextInitialized() {
        execute("CREATE TABLE users (user_id uuid PRIMARY KEY, name text)")
    }

    @Test(groups = "short")
    public void should_map_groovy_class() {
        def mapper = new MappingManager(session()).mapper(User)
        def user1 = new User()
        user1.userId = UUID.randomUUID()
        user1.name = "John Doe"
        mapper.save user1
        def user2 = mapper.get user1.userId
        assertThat(user2.userId).isEqualTo(user1.userId)
        assertThat(user2.name).isEqualTo(user1.name)
    }

}

