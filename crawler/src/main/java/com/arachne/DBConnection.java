package com.arachne;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBConnection {

	private static final Logger logger = LoggerFactory.getLogger(DBConnection.class);

	private static String nodeAddress = null;
	private static final String keyspace = "arachne";

	private static Cluster cluster = null;
	private static Session session = null;


	public static void initialize(String nodeAddress) {
		logger.info("Initializing Cassandra connection to {}", nodeAddress);
		DBConnection.nodeAddress = nodeAddress;
		connect();
	}

	public static void connect() {
		if (session != null) {
			logger.info("Reopening session in connect().");
			session.close();
		}
		if (cluster != null) {
			logger.info("Reconnecting cluster in connect().");
			cluster.close();
		}

		cluster = Cluster.builder()
				.addContactPoint(nodeAddress)
				.build();
		session = cluster.connect(keyspace);
	}

	public static void close() {
		logger.info("Disconnecting cassandra cluster at {}", nodeAddress);
		if (session != null)
			session.close();
		if (cluster != null)
			cluster.close();
	}

	public static String describeCluster() {
		Metadata metadata = cluster.getMetadata();
		String description = "";
	    description += "Connected to cluster: " + metadata.getClusterName() + "\n";
	    for ( Host host : metadata.getAllHosts() ) {
	    	description += "Datacenter: " + host.getDatacenter() +
	    			", Host: " + host.getAddress() +
	    			", Rack: " + host.getRack() + "\n";
	    }
	    return description;
	}

	public static Session getSession() {
		return session;
	}

	public static Cluster getCluster() {
		return cluster;
	}



}
