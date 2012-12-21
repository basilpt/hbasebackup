package com.bizosys.oneline.maintenance;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class StringUtils {
	  
	 public static final String Empty = ""; 
	 
	  /**
	   * Make a string representation of the exception.
	   * @param e The exception to stringify
	   * @return A string with exception name and call stack.
	   */
	  public static String stringifyException(Throwable e) {
	    StringWriter stm = new StringWriter();
	    PrintWriter wrt = new PrintWriter(stm);
	    e.printStackTrace(wrt);
	    wrt.close();
	    return stm.toString();
	  }

	  /**
	   * Checks if a string is empty
	   * @param text String value to check
	   * @return true/false.
	   */
	  public static final boolean isEmpty (String text) {
	    if (null == text ) return true;
	    if ( text.length() == 0 ) return true;
	    else return false;
	  }
	  
	  /**
	   * Given an array of strings, return a comma-separated list of its elements.
	   * @param strs Array of strings
	   * @return Empty string if strs.length is 0, comma separated list of strings
	   * otherwise
	   */
	  
	  public static String arrayToString(String[] strs) {
	    return arrayToString(strs, ',');
	  }
	  
	  public static String arrayToString(String[] strs, char delim) {
	    if (strs == null || strs.length == 0) { return ""; }
	    StringBuffer sbuf = new StringBuffer();
	    sbuf.append(strs[0]);
	    for (int idx = 1; idx < strs.length; idx++) {
	      sbuf.append(delim);
	      sbuf.append(strs[idx]);
	    }
	    return sbuf.toString();
	 }
	  
	  public static String listToString(List<String> strs, char delim) {
		    if (strs == null) return ""; 
		    int len = strs.size();
		    if ( len == 0 )return ""; 
		    
		    StringBuffer sbuf = new StringBuffer();
		    sbuf.append(strs.get(0));
		    for (int idx = 1; idx < len; idx++) {
		      sbuf.append(delim);
		      sbuf.append(strs.get(idx));
		    }
		    return sbuf.toString();
	  }
	  	  

	  /**
	   * returns an arraylist of strings  
	   * @param str the comma seperated string values
	   * @return the arraylist of the comma seperated string values
	   */
	  public static String[] getStrings(String str){
		  return getStrings(str, ",");
	  }

	  /**
	   * returns an arraylist of strings  
	   * @param str the delimiter seperated string values
	   * @param delimiter
	   * @return the arraylist of the comma seperated string values
	   */
	  public static String[] getStrings(String str, String delimiter){
	    if (isEmpty(str)) return null;
	    StringTokenizer tokenizer = new StringTokenizer (str,delimiter);
	    List<String> values = new ArrayList<String>();
	    while (tokenizer.hasMoreTokens()) {
	      values.add(tokenizer.nextToken());
	    }
	    return (String[])values.toArray(new String[values.size()]);
	  }

	  public static String[] getStrings (String line, char delim) {
		  String[] result = new String[]{ line, null };
		  if (line == null) return result;
		  
		  int splitIndex = line.indexOf(delim);
		  if ( -1 != splitIndex) {
				result[0] = line.substring(0,splitIndex);
				if ( line.length() > splitIndex )
				result[1] = line.substring(splitIndex+1);
		  }
		  return result;
	  }
}
