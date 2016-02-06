package com.arachne;

import com.datastax.driver.core.*;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

public class RobotsCheck{

    private static Cluster cluster;
    private static Session session;

    private static final String TABLE_NAME = "robots";


    /**
     *A function to check whether the database already contains a set of rules for the given domain
     * @param domain The domain about to get its rules checked
     * @return True if the domain is already in the database
     */
    public static boolean checkDomain(String domain){
        Statement select = QueryBuilder.select("domain").from(TABLE_NAME).where(eq("domain", domain));
        ResultSet results = session.execute(select);

        return !results.all().isEmpty();
    }

    public static String getRules(String domain){
        Statement select = QueryBuilder.select("rules").from(TABLE_NAME).where(eq("domain", domain));

        /*
        * executes the select statement then gets results as a set, then gets a List of
        * Rows, gets the first row, and gets the value of the rules column as a String
        */
        String rules = session.execute(select).all().get(0).getString("rules");

        return rules;

    }

    public static void main (String[] args){
        cluster = Cluster.builder().addContactPoint("127.0.0.1)").build();
        session = cluster.connect("robots");

        //TODO: implement url retrieval from Storm topology
        String url = "";
        args[0] = url;
        String domain = URLRegExTest.getDomain(url);
    }
}