package edu.illinois.lis.hadoop;

import edu.umd.cloud9.collection.trec.TrecDocument;

public class TrecUtils {
	/**
	 * Get the text element
	 */
	public static String getText(TrecDocument doc) {

		String text = "";
		String content = doc.getContent();
		int start = content.indexOf("<TEXT>");
		if (start == -1) {
			text = "";
		} else {
			int end = content.indexOf("</TEXT>", start);
			text= content.substring(start + 6, end).trim();
		}
		return text;
	}
	
	   public static String getDocNo(TrecDocument doc) {

	        String text = "";
	        String content = doc.getContent();
	        int start = content.indexOf("<DOCNO>");
	        if (start == -1) {
	            text = "";
	        } else {
	            int end = content.indexOf("</DOCNO>", start);
	            text= content.substring(start + 7, end).trim();
	        }
	        return text;
	    }
}
