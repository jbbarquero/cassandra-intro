package com.malsolo.cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class GettingStartedTwo {

    public static void main(String[] args) {
        Cluster cluster = Cluster.builder()
                .addContactPoint("localhost")
                .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
                .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
                .build();
        Session session = cluster.connect("my_keyspace");

        PreparedStatement pst = session.prepare(
                "INSERT INTO user (last_name, first_name, age, city, email) VALUES (?, ?, ?, ?, ?)"
        );

        BoundStatement boundStatement = new BoundStatement(pst);

        session.execute(boundStatement.bind("Jones", "Mr", 35, "Boston", "mr_jones@example.com"));

        Statement select = QueryBuilder.select().all().from("user").where(eq("last_name", "Jones"));

        ResultSet results = session.execute(select);

        results.forEach(row -> System.out.printf("%s %d %n", row.getString("first_name"), row.getInt("age")));

        Statement update = QueryBuilder.update("user").with(QueryBuilder.set("age", 36)).where(eq("last_name", "Jones"));

        session.execute(update);

        session.execute(select).forEach(GettingStartedTwo::printRow);

        cluster.close();
    }

    private static void printRow(Row row) {
        System.out.printf("%s %d %n", row.getString("first_name"), row.getInt("age"));
    }

}
