package com.pateo.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TestProperties {

	
	public static void main(String[] args) throws IOException {
		System.out.println(" this is the begain !");
        Properties prop = new Properties();  

//      // 方式一  配置文件的路径要和 src同一级目录下
		InputStream ips = new FileInputStream("my.properties");
		 try {
			 prop.load(ips);
			 System.out.println(" num is " +prop.getProperty("num"));;
			 
 		} catch (IOException e) {
			e.printStackTrace();
		}
		
//		方式二： 一定要记住用完整的路径，但完整的路径不是硬编码，而是运算出来的。*/
//	InputStream ips2 = new FileInputStream("D:\\java-scala-mixed\\workspace\\cloudplatformshow\\my.properties");
//	 prop.load(ips2);
//	 System.out.println(" num is " +prop.getProperty("num"));;

  
	}
}
