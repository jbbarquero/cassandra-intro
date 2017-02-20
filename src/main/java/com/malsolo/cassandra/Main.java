package com.malsolo.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class Main {

    public static void main(String[] args) {
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

    }

}
