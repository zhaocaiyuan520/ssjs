package redis.com.e_chinalife.rtcs_cd.redisAPI;

import java.util.HashSet;
import java.util.Set;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisSentinelPool;

public class ConnectRedis {
	
	/**
	 * redis主从模式
	 * @param redis_host
	 * @param master_name
	 * @return
	 */
	public JedisSentinelPool redisMasterSlave(String redis_host,String master_name){
		//将连接信息拆分出来
		String[] redis_hosts = redis_host.split(";");
		Set<String> sentinels = new HashSet<String>();
		for(String host : redis_hosts){
			sentinels.add(host);
		}
		JedisPoolConfig poolConfig = new JedisPoolConfig();  
        poolConfig.setMaxIdle(10);  
        poolConfig.setMaxTotal(100);  
        poolConfig.setMaxWaitMillis(2000);  
        poolConfig.setTestOnBorrow(true); 
        return new JedisSentinelPool(master_name, sentinels,poolConfig,100000);
	}
	
	
	public void returnRedis(){
		
	}
	/**
	 * redis集群模式
	 * @param redis_host
	 * @return
	 */
	public JedisCluster redisCluster(String redis_host){
		JedisPoolConfig poolConfig = new JedisPoolConfig();
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        //将连接信息拆分出来
  		String[] redis_hosts = redis_host.split(";");
  		for(String host : redis_hosts){
  			String[] ip_port = host.split(":");
  			nodes.add(new HostAndPort(ip_port[0], Integer.parseInt(ip_port[1])));
  		}
        return new JedisCluster(nodes, poolConfig);//JedisCluster中默认分装好了连接池.
	}
	
	/**
	 * redis单机模式
	 * @param redis_host
	 * @return
	 */
	public Jedis redisSingle(String redis_host){
		String[] ip_port = redis_host.split(":");
		// 池基本配置 
        JedisPoolConfig config = new JedisPoolConfig(); 
        config.setMaxIdle(10);  
        config.setMaxTotal(100);  
        config.setMaxWaitMillis(2000);  
        config.setTestOnBorrow(true); 
        
        JedisPool jedisPool = new JedisPool(config,ip_port[0],Integer.parseInt(ip_port[1]));
		return jedisPool.getResource();
	}

}
