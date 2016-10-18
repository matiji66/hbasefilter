package com.pateo.utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ResourceBundle;

import com.pateo.constant.Constant;

public class PropertyUtils {

	static Map<String, String> prop = null;
	static String  propertyName ="redis";
	public static void main(String[] args) {
	
		
		System.out.println(Constant.hbase_zookeeper_quorum);
        System.out.println( "zookeeper.quorum : " + PropertyUtils.getValue(Constant.hbase_zookeeper_quorum));;
 
		Properties prop = new Properties();     
         try{
             //读取属性文件a.properties
//        	 InputStream in = PropertyUtils.class.getClass().getClassLoader().getResourceAsStream("b");   

             InputStream in = new BufferedInputStream (new FileInputStream("b.properties"));
             prop.load(in);     ///加载属性列表
             Iterator<String> it=prop.stringPropertyNames().iterator();
             while(it.hasNext()){
                 String key=it.next();
                 System.out.println(key+":"+prop.getProperty(key));
             }
             in.close();
             
             ///保存属性到b.properties文件
             FileOutputStream oFile = new FileOutputStream("b.properties", true);//true表示追加打开
             prop.put("phone2", "1231231231");
//             prop.store(oFile, "The New properties file");
             prop.store(oFile, "The New properties file");
             System.out.println("The New  properties file");
             oFile.close();
         }
         catch(Exception e){
             System.out.println(e);
		 }
		
	}
	
	/** 
	 * 获取指定配置文件中所以的数据 
	 * @param propertyName 
	 *        调用方式： 
	 *            1.配置文件放在resource源包下，不用加后缀 
	 *              PropertiesUtil.getAllMessage("message"); 
	 *            2.放在包里面的 
	 *              PropertiesUtil.getAllMessage("com.test.message"); 
	 * @return 
	 */  
	public static List<String> getAllMessage(String propertyName) {  
	    // 获得资源包  
	    ResourceBundle rb = ResourceBundle.getBundle(propertyName.trim());  
	    // 通过资源包拿到所有的key  
	    Enumeration<String> allKey = rb.getKeys();  
	    // 遍历key 得到 value  
	    List<String> valList = new ArrayList<String>();  
	    while (allKey.hasMoreElements()) {  
	        String key = allKey.nextElement();  
	        String value = (String) rb.getString(key);  
	        valList.add(value);  
	    }  
	    return valList;  
	}  
	
	public static Map<String,String> getAllMessageMap(String propertyName) {  
	    // 获得资源包  
	    ResourceBundle rb = ResourceBundle.getBundle(propertyName.trim()); 
	     
	    // 通过资源包拿到所有的key  
	    Enumeration<String> allKey = rb.getKeys();  
	    // 遍历key 得到 value    
	    while (allKey.hasMoreElements()) {  
	        String key = allKey.nextElement();  
	        String value = (String) rb.getString(key);
//	        System.out.println("key : " + key + " value :" + value);
	        prop.put(key, value);
	    }  
	    return prop;  
	}  
  
	public static String getValue(String key) {
		
		if (null == prop ) {
			prop = new HashMap<String, String>();
			getAllMessageMap(propertyName);
		}
		return  prop.get(key);
	}
	
}

