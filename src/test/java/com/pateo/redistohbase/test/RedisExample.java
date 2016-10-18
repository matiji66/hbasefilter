package com.pateo.redistohbase.test;

import java.util.Queue;  
import java.util.Set;  
import java.util.concurrent.ConcurrentMap;  
  




import org.redisson.Config;  
import org.redisson.Redisson;  
import org.redisson.core.RMap;

import com.pateo.utils.PropertyUtils;
  
public class RedisExample {  
  
    /** 
     * @param args 
     */  
    public static void main(String[] args) {  
        // 1.初始化  
        Config config = new Config();  
        config.setConnectionPoolSize(10);  
//        config.addAddress("127.0.0.1:6379"); 
        config.addAddress("10.172.10.155:63791");  
//        redis.host=obd-redis.marathon.pateo.cn
//        		redis.port=63791
        PropertyUtils.getValue("redis.host");
        System.out.println("redis.host"+ PropertyUtils.getValue("redis.host"));
        Redisson redisson = Redisson.create(config);  
        System.out.println("reids连接...");  
  
        // 2.测试concurrentMap,put方法的时候就会同步到redis中  
//        obd_data_id_P006000300007695
//        ConcurrentMap<String, Object> map = redisson.getMap("FirstMap");  
//        map.put("wuguowei", "男");  
//        map.put("zhangsan", "nan");  
//        map.put("lisi", "女");  
//        ConcurrentMap<String, String> resultMap = redisson.getMap("FirstMap");
//        System.out.println("resultMap==" + resultMap.keySet());  
        redisson.getMap("obd_data_id_P006000300007695");
        RMap<Object, Object> map2 = redisson.getMap("obd_data_id_P006000300007695");
        System.out.println("resultMap==" + map2.keySet());  
//        // 2.测试Set集合  
//        Set<String> mySet = redisson.getSet("MySet");  
//        mySet.add("wuguowei");  
//        mySet.add("lisi");  
//  
//        Set<?> resultSet = redisson.getSet("MySet");  
//        System.out.println("resultSet===" + resultSet.size());  
//          
//        //3.测试Queue队列  
//        Queue<String> myQueue = redisson.getQueue("FirstQueue");  
//        myQueue.add("wuguowei");  
//        myQueue.add("lili");  
//        myQueue.add("zhangsan");  
//        myQueue.peek();  
//        myQueue.poll();  
//  
        Queue<?> resultQueue=redisson.getQueue("obd_data_id_P006000300007695");  
        System.out.println("resultQueue==="+resultQueue);  
          
        // 关闭连接  
        redisson.shutdown();  
    }  
  
}  