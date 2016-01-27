package com.arachne;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;

/**
 * @class URLQueue
 * @description Represents the list of recently processed URLs by all the nodes
 * @author Ravi Pandya
 * @written 23 January 2016
 */
public class URLQueue{
	/**
	 * A linked list representation of the recently processed URLs
	 */
	private LinkedList<String> urls = new LinkedList<String>();
	
	/**
	 * A HashMap with each distinct domain paired with the number of times it has been accessed
	 * recently
	 */
	private HashMap<String, Integer> distribution = new HashMap();
	
	/**
	 * The total number of URLs stored in the recent queue
	 */
	private int totalURLs = 0;
	
	/**
	 * The maximum length of the queue, or what will be considered 'recent'
	 */
	private int maxLength;
	
	/**
	 * The current length of the queue
	 */
	private int length;
	
	/**
	 * Static method to extract a domain name from any given URL
	 * @param url The URL to be processed
	 * @return domain The domain of the URL passed in
	 */
	public static String getDomain(String url){
		/*TODO: use regex to determine the domain of a URL*/
		return url;
	}
	
	/**
	 * Constructor for a new URLQueue taking in no previously processed URLs
	 * @param maxLength What will be considered 'recent' for this queue
	 */
	public URLQueue(int maxLength){
		this.length = 0;
		this.maxLength = maxLength;
	}
	
	/**
	 * Constructor 
	 * @param maxLength What will be considered 'recent' for this queue
	 * @param urls A LinkedList of URLs that have already been processed
	 */
	public URLQueue(int maxLength, LinkedList<String> urls){
		this.urls = urls;
		this.length = urls.size();
		this.maxLength = maxLength;
	}
	
	/**
	 * Method for adding a new URL to the list of recently processed URLs, pops the last 
	 * element off of the queue if the queue has reached its maximum length
	 * @param url the URL that has just been processed and must be added to the stack
	 */
	public void add(String url){
		String domain = getDomain(url);
		
		if(this.length >= maxLength){
			String last = this.urls.getLast();
			this.distribution.put(last, this.distribution.get(last)-1);
			this.urls.removeLast();
		}
		
		this.urls.add(0, url);
		this.length = urls.size();
		
		if(this.distribution.containsKey(domain)){
			this.distribution.put(domain, this.distribution.get(domain)+1);
			this.totalURLs += 1;
		} else {
			this.distribution.put(domain, 0);
		}
	}
	
	/**
	 * Returns the percentage recent URLs processed this domain represents
	 * @param domain The domain to be processed
	 * @return 
	 */
	public double percentage(String domain){
		return (double)(this.distribution.get(domain))/this.totalURLs*100;
	}
}
