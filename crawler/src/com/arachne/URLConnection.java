package com.arachne;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;

public class URLConnection {

	private String nodeAddress = null;
	private final String keyspace = "arachne";

	private Cluster cluster = null;
	private Session session = null;

	public URLConnection(String nodeAddress) {
		this.nodeAddress = nodeAddress;
	}

	public void connect() {
		if (session != null)
			session.close();
		if (cluster != null)
			cluster.close();

		cluster = Cluster.builder()
				.addContactPoint(nodeAddress)
				.build();
		session = cluster.connect(keyspace);
	}

	public void close() {
		if (session != null)
			session.close();
		if (cluster != null)
			cluster.close();
	}

	public String describeCluster() {
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

	public Session getSession() {
		return session;
	}

	public Cluster getCluster() {
		return cluster;
	}

}
