package com.malsolo.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GettingStartedOne {

    final static Logger logger = LoggerFactory.getLogger(GettingStartedOne.class);

    public static void main(String[] args) {
        logger.info("Cassandra getting started with Java, part I.");

        Cluster cluster;
        Session session;

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("my_keyspace");

        session.execute("INSERT INTO user (last_name, first_name, age, city, email) VALUES ('Smith', 'John', 69, 'New York', 'paco@example.com')");
        ResultSet results = session.execute("SELECT * FROM user WHERE last_name='Jones'");
        results.forEach(u -> System.out.format("%s %d\n", u.getString("first_name"), u.getInt("age")));

        session.execute("update user set age = 36 where last_name = 'Jones'");
        results = session.execute("select * from user where last_name='Jones'");
        results.forEach(u -> System.out.format("%s %d\n", u.getString("first_name"), u.getInt("age")));

        cluster.close();

        logger.info("Cassandra getting started with Java, part I. Done!");
    }

}
