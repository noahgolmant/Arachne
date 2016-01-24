package com.arachne;


import java.util.HashMap;
import java.util.LinkedList;

public class URLQueue{
	
	private LinkedList<String> urls = new LinkedList<String>();
	private HashMap<String, Integer> distribution = new HashMap();
	private int maxLength;
	private int length;
	
	public URLQueue(int maxLength){
		this.length = 0;
		this.maxLength = maxLength;
	}
	
	public URLQueue(int maxLength, LinkedList<String> urls){
		this.urls = urls;
		this.length = urls.size();
		this.maxLength = maxLength;
	}
	
	public String getDomain(String url){
		return url;
	}
	
	public void add(String url){
		String domain = this.getDomain(url);
		
		if(this.length >= maxLength){
			this.urls.removeLast();
		}
		this.urls.add(0, url);
		this.length = urls.size();
		
		if(this.distribution.containsKey(domain)){
			this.distribution.put(domain, this.distribution.get(domain)+1);
		} else {
			this.distribution.put(domain, 0);
		}
	}
}
