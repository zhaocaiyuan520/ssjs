package redis.com.e_chinalife.rtcs_cd.redisAPI;

import java.util.Map;
import java.util.TreeSet;

import com.sun.istack.internal.logging.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;

public class RedisOperator implements IRedisOperator {


	public TreeSet<String> keys(String pattern,JedisCluster jedisCluster){
		TreeSet<String> keys = new TreeSet<String>();
		Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
		for(String k : clusterNodes.keySet()){
			JedisPool jp = clusterNodes.get(k);
			Jedis connection = jp.getResource();
			try {
				keys.addAll(connection.keys(pattern));
			} catch(Exception e){
			} finally{
				connection.close();//用完一定要close这个链接！！！
			}
		}
		return keys;
	}
}
