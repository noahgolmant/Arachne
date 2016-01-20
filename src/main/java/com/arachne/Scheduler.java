package com.arachne;

/**
 * @class Scheduler
 * @description Distributes URLs to process across cluster.
 * @author Noah Golmant
 * @written 19 Jan 2016
 */
public class Scheduler {
	
	/**
	 * Scheduler singleton instance.
	 */
	private static Scheduler instance = null;
	
	/**
	 * Number of URLS to retrieve and distribute across cluster.
	 */
	private final int NUM_URLS_TO_RETRIEVE = 1000;
	
	/**
	 * Number of nodes in compute cluster.
	 */
	private final int NUM_NODES = 4;
	
	/**
	 * Connection to stream of URLs from database
	 */
	private URLConnection urlConnection = null;
	
	/**
	 * Address of node in cluster to which we connect
	 */
	private String nodeAddress = "127.0.0.1"; /* TODO: get cluster address on startup */
	
	/**
	 * Protected scheduler singleton constructor.
	 */
	protected Scheduler() {
		urlConnection = new URLConnection(nodeAddress);
		urlConnection.connect();
		System.out.println(urlConnection.describeCluster());
	}
	
	/**
	 * Get URLs to process and distribute to cluster
	 * @return list of URLs to process
	 */
	private String[] getURLs() {		
		return null;
	}
	
	/**
	 * Distribute URLs to process across the worker nodes.
	 * @param urls URLs to process
	 * @param nodes Addresses of worker nodes.
	 */
	private void distributeURLs(String[] urls, String[] nodes) {
		
	}
	
	public static void main(String[] args) {
		Scheduler.getInstance();
	}
	
	/**
	 * Scheduler singleton access method.
	 */
	public static Scheduler getInstance() {
		if (instance == null) {
			instance = new Scheduler();
		}
		return instance;
	}
}
