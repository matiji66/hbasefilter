package com.pateo.redistohbase.test;

import org.apache.hadoop.hbase.util.Bytes;
 
/**
 * Hello world!
 *
 */
public class App 
{
	 
	    public static void main(String[] args){
	    	
	    	System.out.println("--------test-----");
 
	    	byte[] bytes = Bytes.toBytes(45.0);
	    	
			System.out.println("dasdasda---" +Bytes.toLong(bytes));;

	        
	    }
}
