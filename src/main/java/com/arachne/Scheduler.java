package com.arachne;

import java.util.AbstractQueue;
import java.util.HashMap;
import java.util.Queue;
import java.util.Set;

/**
 * @class Scheduler
 * @description Distributes URLs to process across cluster.
 * @author Noah Golmant
 * @author Ravi Pandya
 * @written 19 Jan 2016
 */
public class Scheduler {
	
	/**
	 * The queue of URLs yet to be processed
	 */
	private URLQueue toProcess = new URLQueue();
	
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
	 * A map of domain names to the number of times it appears in the recently processed URLs
	 */
	private HashMap<String, Integer> commonDomains = new HashMap();
	
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
	private String[] getURLs() {		/*TODO: delete, uses stream and gets single URL at a time*/
		return null;
	}
	
	/**
	 * @param url A raw URL to be parsed
	 * @return domain The domain of the URL passed in
	 */
	private String getDomain(String url){
		boolean found = false;
		String domain = "";
		
		//searches the URL until if finds a / 
		for(int i=0; i < url.length(); i++){
			if(url.charAt(i) != '/'){
				url = url.substring(1);
			} else {
				found = true;
				break;
			}
		}
		
		if(found){
			char c = url.charAt(2);
			while(c != '/'){
				domain += c;
			}
			return domain;
		}
		return null;
	}
	
	/**
	 * Distribute URL to a single node
	 * @param url URL to process
	 * @param nodes Addresses of worker nodes and associated arrays of recently processed URLs
	 */
	private void distributeURL(String url, HashMap<String, String[]> nodes) {
		Set<String> nodeAddresses = nodes.keySet();
		String newDomain = getDomain(url);
		
		if(commonDomains.get(newDomain) > 2){
			
		}
		
		for(String address: nodeAddresses){
			
		}
		
		/*TODO: put in separate update function*/
		for (String address: nodeAddresses){
			String[] recents = nodes.get(address);
			for(int i=0; i < recents.length; i++){
				String domain = getDomain(recents[i]);
				if(commonDomains.containsKey(domain)){
					//if it already exists, increments the counter for that domain by one
					commonDomains.put(domain, commonDomains.get(domain)+1); 
				} else {
					//inserts the new domain to the HashMap of common domains
					commonDomains.put(domain, 0);
				}
			}
		}
		
		
		
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
