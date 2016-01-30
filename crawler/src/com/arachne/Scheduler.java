package com.arachne;

import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @class Scheduler
 * @description Distributes URLs to process across cluster.
 * @author Noah Golmant
 * @author Ravi Pandya
 * @written 19 January 2016
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
	 * The threshold as a percent for deciding whether a domain appears in the queue too often
	 */
	private final double OVERLOAD_THRESHOLD = 15;
	
	/**
	 * The number of URLs to be considered recent
	 */
	private final int RECENT = 100;
	
	/**
	 * The group number in the URL RegEx that refers to the domain name (.+?(?=\\.))
	 */
	private final int URL_REGEX_GROUP_NO = 6;

	/**
	 * An array containing the addresses of all of the nodes
	 */
	private String[] nodeAddresses = new String[NUM_NODES];
	
	/**
	 * The index of the node that will be assigned a URL to process
	 */
	private int currNode = 0;
	
	/**
	 * HashMap containing the node number and the domain it last processed
	 */
	private HashMap<Integer, String> lastProcessed = new HashMap<Integer, String>();
	
	/**
	 * The queue of URLs yet to be processed
	 */
	private URLQueue recentURLs = new URLQueue(RECENT);
	
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
	private String getURLs() {
		return null;
	}
	
	/**
	 * @param url A raw URL to be parsed
	 * @return domain The domain of the URL passed in
	 */
	private String getDomain(String url){
		//RegEx to check if URL matches the correct format, parentheses are for grouping
		Pattern p = Pattern.compile("(https?:)(\\/)(\\/)(w{3})(\\.)(.+?(?=\\.))(\\.)([a-z]{2}[a-z]{1})(.*)");
		Matcher m = p.matcher(url);
		
		String domain = "";
		if(m.find()){
			domain = m.group(URL_REGEX_GROUP_NO);
			return domain;
		}
		return "Domain not found";
	}
	
	
	/**
	 * Distribute URL to a single node
	 * @param url URL to process
	 * @param nodes Addresses of worker nodes and associated arrays of recently processed URLs
	 */
	private void distributeURL(String url) {
		String domain = getDomain(url);
		
		if(recentURLs.percentage(domain) > OVERLOAD_THRESHOLD){
			/*TODO shuffle URL back into URL stream to be processed*/
			return;
		}
		
		/* Checks if the current node last processed the same domain name, if so, then it will 
		 * check the subsequent nodes until it finds one that did not, as part of the politeness 
		 * policy. If all nodes last went to this domain, the URL will be shuffled to the back 
		 * of the URLQueue. 
		 */
		if(lastProcessed.get(currNode) == domain){
			int i = currNode + 1;

			while(i != currNode){
				if(i == NUM_NODES){i=0;}
				if(lastProcessed.get(i) != domain){
					lastProcessed.put(i, domain);
					currNode += 1;
					if(currNode == NUM_NODES){currNode=0;}
					/*TODO: actually distribute the URL to the current node*/
					recentURLs.add(url);
					return;
				} else {
					
				}
			}
		} else {
			lastProcessed.put(currNode, domain);
			/*TODO: actually distribute the URL to the current node*/
			recentURLs.add(url);
			return;
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
