package com.arachne;

import java.util.regex.*;

public class URLRegExTest {
	public static String getDomain(String url){	
		Pattern p = Pattern.compile("(https?:)(\\/)(\\/)(w{3})(\\.)(.+?(?=\\.))(\\.)([a-z]{2}[a-z]{1})");
		Matcher m = p.matcher(url);
		
		String found = "";
		if(m.find()){
			found = m.group(6);
			return found;
		}
		return "Domain not found";
		
	}
	
	public static String test(String url){
		Pattern p = Pattern.compile("(\\d)(\\d)");
		Matcher m = p.matcher(url);
		
		m.find();
		return m.group(1);
	}
	public static void main(String[] args){
		System.out.println(getDomain("http://www.noahgolmant.com"));
	}
}
