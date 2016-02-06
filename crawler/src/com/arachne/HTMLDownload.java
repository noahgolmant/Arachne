package com.arachne;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.select.Elements;

import java.io.IOException;

public class HTMLDownload{
    private static void getLinks(Document doc){
        Elements links = doc.select("a[href]");
    }


    public static void main(String[] args) throws IOException{

    }
}