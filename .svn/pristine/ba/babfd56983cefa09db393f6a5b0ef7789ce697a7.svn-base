package redis.com.e_chinalife.rtcs_cd.redisAPI;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;
import redis.com.e_chinalife.rtcs_cd.redisListener.RedisListener;

public class OperationRedis {
	
	private Jedis jedisSingle;//单个客户端连接
	
	private Jedis jedisMasterSlave;//主从客户端连接
	
	private JedisCluster jedisCluster;//进群客户端连接
	
	private String redisType;//redis类型（0：单机模式；1：主从模式；2：集群模式）
	
	public OperationRedis(){
		InputStream in = null;
		try {
			in = Object. class .getResourceAsStream( "/redis.properties" );
			Properties prop =  new  Properties();    
			prop.load(in);
			//获取redis的连接信息
			//例子:MasterSlave,RtcsCdMaster,10.253.129.251:26379;10.31.23.73:26379
			//例子:Cluster,10.31.23.77:6380;10.31.23.77:6381;10.31.23.77:6382;10.31.23.77:6383;10.31.23.77:6384
			String redis_information=prop.getProperty( "redis.information" ).trim();
			ConnectRedis connectRedis = new ConnectRedis();
			if(redis_information!=null){
				String[] redis_information_list = redis_information.split(",");
				String type=redis_information_list[0];
				String master_name="";
				String redis_host="";
				if(type.equals("Single")){
					redis_host=redis_information_list[1];
					jedisSingle=connectRedis.redisSingle(redis_host);
					this.redisType="0";
				}else if(type.equals("MasterSlave")){
					master_name=redis_information_list[1];
					redis_host=redis_information_list[2];
					jedisMasterSlave=connectRedis.redisMasterSlave(redis_host, master_name).getResource();
					this.redisType="1";
				}else if(type.equals("Cluster")){
					redis_host=redis_information_list[1];
					jedisCluster=connectRedis.redisCluster(redis_host);
					this.redisType="2";
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}finally{
			try {
				if(in!=null){
					in.close();
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 传入参数连接redis
	 * @param redis_information
	 * 例子:MasterSlave,RtcsCdMaster,10.253.129.251:26379;10.31.23.73:26379
	 * 例子:Cluster,10.31.23.77:6380;10.31.23.77:6381;10.31.23.77:6382;10.31.23.77:6383;10.31.23.77:6384
	 */
	public OperationRedis(String redis_information){
		ConnectRedis connectRedis = new ConnectRedis();
		if(redis_information!=null){
			String[] redis_information_list = redis_information.split(",");
			String type=redis_information_list[0];
			String master_name="";
			String redis_host="";
			if(type.equals("Single")){
				redis_host=redis_information_list[1];
				jedisSingle=connectRedis.redisSingle(redis_host);
				this.redisType="0";
			}else if(type.equals("MasterSlave")){
				master_name=redis_information_list[1];
				redis_host=redis_information_list[2];
				jedisMasterSlave=connectRedis.redisMasterSlave(redis_host, master_name).getResource();
				this.redisType="1";
			}else if(type.equals("Cluster")){
				redis_host=redis_information_list[1];
				jedisCluster=connectRedis.redisCluster(redis_host);
				this.redisType="2";
			}
		}
	}
	
	/**
	 * 订阅得到信息在lister的onPMessage(...)方法中进行处理  
	 * @param expiredListener
	 */
	 public void psubscribe(RedisListener expiredListener){
	    if (this.redisType.equals("0"))
	    {
	      this.jedisSingle.psubscribe(expiredListener, new String[] { "__key*__:*"});
	    }
	    else if (this.redisType.equals("1"))
	    {
	      this.jedisMasterSlave.psubscribe(expiredListener, new String[] { "__key*__:*"});
	    }
	    else if (this.redisType.equals("2"))
	    {
	      this.jedisCluster.psubscribe(expiredListener, new String[] { "__key*__:*"});
	    }
	  }
	 
	/**
	 * 获取模糊查询的数据，以json的形式传出
	 * @param str
	 * @return
	 */
	public JSONObject vague_json(String keys){
		JSONObject params = new JSONObject();
		Set<String> fields = null;
		if(redisType.equals("0")){
			fields = jedisSingle.keys(keys);
		}else if(redisType.equals("1")){
			fields = jedisMasterSlave.keys(keys);
		}else if(redisType.equals("2")){
			fields = new RedisOperator().keys(keys,jedisCluster);
		}
		if(fields!=null&&fields.size()>0){
			Object[] keysObjs = fields.toArray();
			for(Object keysObj : keysObjs){
				String key=keysObj.toString();
				try {
					if(redisType.equals("0")){
						params.put(key, jedisSingle.get(key));
					}else if(redisType.equals("1")){
						params.put(key, jedisMasterSlave.get(key));
					}else if(redisType.equals("2")){
						params.put(key, jedisCluster.get(key));
					}
				} catch (JSONException e) {
					e.printStackTrace();
				}
			}
		}
		return params;
	}
	
	/**
	 * 模糊查询，获取金额的和
	 * @param key
	 * @return
	 */
	public String sumVague(String key){
		
		Set<String> fields = null;
		if(redisType.equals("0")){
			fields = jedisSingle.keys(key);
		}else if(redisType.equals("1")){
			fields = jedisMasterSlave.keys(key);
		}else if(redisType.equals("2")){
			fields = new RedisOperator().keys(key,jedisCluster);
		}
		if(fields!=null&&fields.size()>0){
			Object[] keysObjs = fields.toArray();
			double sumtval=0;
			for(Object keysObj : keysObjs){
				String val = null;
				if(redisType.equals("0")){
					val = jedisSingle.get(keysObj.toString());
				}else if(redisType.equals("1")){
					val = jedisMasterSlave.get(keysObj.toString());
				}else if(redisType.equals("2")){
					val = jedisCluster.get(keysObj.toString());
				}
				if(val!=null){
					sumtval=sumtval+Double.parseDouble(val);
				}
			}
			if(sumtval>0){
				return sumtval+"";
			}
		}

		return "0.00";
	}
	
	/**
	 * 模糊查询获取总数的和
	 * @param key
	 * @return
	 */
	public String countVague(String key){
		Set<String> fields = null;
		if(redisType.equals("0")){
			fields = jedisSingle.keys(key);
		}else if(redisType.equals("1")){
			fields = jedisMasterSlave.keys(key);
		}else if(redisType.equals("2")){
			fields = new RedisOperator().keys(key,jedisCluster);
		}
		if(fields!=null&&fields.size()>0){
			Object[] keysObjs = fields.toArray();
			long countval=0;
			for(Object keysObj : keysObjs){
				String val = null;
				if(redisType.equals("0")){
					val = jedisSingle.get(keysObj.toString());
				}else if(redisType.equals("1")){
					val = jedisMasterSlave.get(keysObj.toString());
				}else if(redisType.equals("2")){
					val = jedisCluster.get(keysObj.toString());
				}
				if(val!=null){
					countval=countval+Long.parseLong(val);
				}
			}
			if(countval>0){
				return countval+"";
			}
		}

		return "0";
	}
	
    /**
	 * 判断key值是否存在，存在返回true，不存在返回false
	 * @param key
	 * @return
	 */
	public String exists(String str){//Boolean
		if(redisType.equals("0")){
			return jedisSingle.exists(str).toString();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.exists(str).toString();
		}else if(redisType.equals("2")){
			return jedisCluster.exists(str).toString();
		}
		return null;
	}
	
	public String type(String str){//
		if(redisType.equals("0")){
			return jedisSingle.type(str);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.type(str);
		}else if(redisType.equals("2")){
			return jedisCluster.type(str);
		}
		return null;
	}
	
	/**
	 * 设置key的过期时间
	 * @param key
	 * @param seconds
	 * @return
	 */
	public String expireKey(String key, int seconds){
		if(redisType.equals("0")){
			return jedisSingle.expire(key,seconds).toString();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.expire(key,seconds).toString();
		}else if(redisType.equals("2")){
			return jedisCluster.expire(key,seconds).toString();
		}
		return null;
	}
	
	/**
	 * 启动reids的事务
	 * @return Transaction
	 */
	public Transaction multi(){
		if(redisType.equals("0")){
			return jedisSingle.multi();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.multi();
		}else if(redisType.equals("2")){
			//return jedisCluster.multi();
		}
		return null;
	}
	
	public Pipeline pipelined(){
		if(redisType.equals("0")){
			return jedisSingle.pipelined();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.pipelined();
		}else if(redisType.equals("2")){
			//return jedisCluster.multi();
		}
		return null;
	}
    
    /**
	 * 模糊查询key
	 * @param key 带入*号
	 * @return
	 */
	public List<String> getkeys(String key){//List<String>
		Set<String> fields = null;
		if(redisType.equals("0")){
			fields = jedisSingle.keys(key);
		}else if(redisType.equals("1")){
			fields = jedisMasterSlave.keys(key);
		}else if(redisType.equals("2")){
			fields = new RedisOperator().keys(key,jedisCluster);
		}
		if(fields!=null&&fields.size()>0){
			Object[] fieldsObjs = fields.toArray();
			List<String> fieldsList = new ArrayList<String>();
			for(Object fieldsObj : fieldsObjs){
				fieldsList.add(fieldsObj.toString());
			}
			if(fieldsList!=null&&fieldsList.size()>0){
				return fieldsList;
			}
		}
		return null;
	}
    
    /**
	 * 根据指定的单个key删除数据
	 * @param key
	 * @return
	 */
	public String del(String key){
		if(redisType.equals("0")){
			return jedisSingle.del(key).toString();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.del(key).toString();
		}else if(redisType.equals("2")){
			return jedisCluster.del(key).toString();
		}
		return null;
	}
    
    /**
	 * 根据传过来的值模糊查询，然后删除查询出来的键集合
	 * @param key
	 * @return
	 */
	public Long delVagueKey(String key){
		Set<String> fields = null;
		if(redisType.equals("0")){
			fields = jedisSingle.keys(key);
		}else if(redisType.equals("1")){
			fields = jedisMasterSlave.keys(key);
		}else if(redisType.equals("2")){
			fields = new RedisOperator().keys(key,jedisCluster);
		}
		if(fields!=null&&fields.size()>0){
			Object[] keysObjs = fields.toArray();
			String[] delkeys = new String[fields.size()];
			int i =0;
			for(Object keysObj : keysObjs){
				delkeys[i]=keysObj.toString();
				i++;
			}
			if(key.length()>0){
				if(redisType.equals("0")){
					return jedisSingle.del(delkeys);
				}else if(redisType.equals("1")){
					return jedisMasterSlave.del(delkeys);
				}else if(redisType.equals("2")){
					return jedisCluster.del(delkeys);
				}
			}
		}
		return (long) 0;
	}
    
    /**
     * 添加String类型的数据，如果存在就返回0表示失败，1表示成功
     * @param key
     * @param value
     * @return
     */
	public String setnx(String key,String value){
		if(redisType.equals("0")){
			return jedisSingle.setnx(key, value).toString();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.setnx(key, value).toString();
		}else if(redisType.equals("2")){
			return jedisCluster.setnx(key, value).toString();
		}
		return null;
	}
    
    /**
     * 添加String类型的数据，如果存在就替换原来的数据，返回1表示成功，0表示失败
     * @param key
     * @param value
     * @return
     */
	public String set(String key,String value){
		if(redisType.equals("0")){
			return jedisSingle.set(key, value);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.set(key, value);
		}else if(redisType.equals("2")){
			return jedisCluster.set(key, value);
		}
		return null;
	}
    
    /**
	 * 根据key，获取String类型的值
	 * @param key
	 * @return
	 */
	public String get(String key){
		if(redisType.equals("0")){
			return jedisSingle.get(key);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.get(key);
		}else if(redisType.equals("2")){
			return jedisCluster.get(key);
		}
		return null;
	}
	
	/**
	 * 对key的值做加加操作,并返回新的值。注意incr一个不是int的value会返回错误，incr一个不存在的key，则设置key为1
	 * @param key
	 * @return
	 */
	public Long incr(String key){
		if(redisType.equals("0")){
			return jedisSingle.incr(key);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.incr(key);
		}else if(redisType.equals("2")){
			return jedisCluster.incr(key);
		}
		return null;
	}
	
	/**
	 * 将指定的key增加指定的值（小数）
	 * @param key
	 * @param value
	 * @return
	 */
	public Double incrByFloat(String key,double value){
		if(redisType.equals("0")){
			return jedisSingle.incrByFloat(key,value);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.incrByFloat(key,value);
		}else if(redisType.equals("2")){
			return jedisCluster.incrByFloat(key,value);
		}
		return null;
	}
	
	/**
	 * 使用脚本
	 * @param script
	 * @param keys
	 * @param args
	 * @return
	 */
	public Object eval(String script, List<String> keys, List<String> args){
		if(redisType.equals("0")){
			return jedisSingle.eval(script, keys, args);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.eval(script, keys, args);
		}else if(redisType.equals("2")){
			return jedisCluster.eval(script, keys, args);
			//jedisCluster.scriptLoad(script, key);
			//jedisCluster.evalsha(script, key)
		}
		return null;
	}
	
	public Object publish(String channel, String message){
		if(redisType.equals("0")){
			return jedisSingle.publish(channel, message);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.publish(channel, message);
		}else if(redisType.equals("2")){
			return jedisCluster.publish(channel, message);
			//jedisCluster.scriptLoad(script, key)
		}
		return null;
	}
	
	/**
	 * 对key的值做减减操作,并返回新的值。注意incr一个不是int的value会返回错误，incr一个不存在的key，则设置key为-1
	 * @param key
	 * @return
	 */
	public Long decr(String key){
		if(redisType.equals("0")){
			return jedisSingle.decr(key);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.decr(key);
		}else if(redisType.equals("2")){
			return jedisCluster.decr(key);
		}
		return null;
	}
    
	/**
	 * 单个添加map类型的数据
	 * @param key
	 * @param field
	 * @param value
	 * @return
	 */
	public String hset(String key,String field,String value){
		if(redisType.equals("0")){
			return jedisSingle.hset(key,field,value).toString();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.hset(key,field,value).toString();
		}else if(redisType.equals("2")){
			return jedisCluster.hset(key,field,value).toString();
		}
		return null;
	}
    
    /**
     * 批量添加map类型的数据
     * @param key
     * @param map
     * @return
     */
	public String hmset(String key,HashMap<String, String> map){
		if(redisType.equals("0")){
			return jedisSingle.hmset(key, map);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.hmset(key, map);
		}else if(redisType.equals("2")){
			return jedisCluster.hmset(key, map);
		}
		return null;
	}
    
    /**
	 * 获取某个map类型数据里面某个key的值
	 * @param key
	 * @return
	 */
	public String hget(String key,String field){//List<String>
		if(redisType.equals("0")){
			return jedisSingle.hget(key, field);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.hget(key, field);
		}else if(redisType.equals("2")){
			return jedisCluster.hget(key, field);
		}
		return null;
	}
    
    /**
	 * 获取某个map类型数据所有的key
	 * @param key
	 * @return
	 */
	public String hkeys(String key){//List<String>
		Set<String> fields = null;
		if(redisType.equals("0")){
			fields = jedisSingle.hkeys(key);
		}else if(redisType.equals("1")){
			fields = jedisMasterSlave.hkeys(key);
		}else if(redisType.equals("2")){
			fields = jedisCluster.hkeys(key);
		}
		if(fields!=null&&fields.size()>0){
			Object[] fieldsObjs = fields.toArray();
			List<String> fieldsList = new ArrayList<String>();
			for(Object fieldsObj : fieldsObjs){
				fieldsList.add(fieldsObj.toString());
			}
			if(fieldsList.size()>0){
				return fieldsList.toString();
			}
		}
		return null;
	}
    
    /**
	 * 获取某个map类型数据所有的value
	 * @param key
	 * @return
	 */
	public List<String> hvals(String key){//List<String>
		if(redisType.equals("0")){
			return jedisSingle.hvals(key);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.hvals(key);
		}else if(redisType.equals("2")){
			return jedisCluster.hvals(key);
		}
		return null;
	}
    
    /**
	 * 获取某个map类型数据所有的key和value
	 * @param key
	 * @return
	 */
	public Map<String, String> hgetAll(String key){//Map<String, String>
		if(redisType.equals("0")){
			return jedisSingle.hgetAll(key);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.hgetAll(key);
		}else if(redisType.equals("2")){
			return jedisCluster.hgetAll(key);
		}
		return null;
	}
	
	public Long hdel(String key,String field){
		if(redisType.equals("0")){
			return jedisSingle.hdel(key,field);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.hdel(key,field);
		}else if(redisType.equals("2")){
			return jedisCluster.hdel(key,field);
		}
		return null;
	}
	
	/**
	 * 获取list类型的数据,下标从0开始，负值表示从后面计算，-1表示倒数第一个元素 ，key不存在返回空列表
	 * @param key
	 * @param start
	 * @param stop
	 * @return
	 */
	public String lrange(String key,int start,int end){
		if(redisType.equals("0")){
			return jedisSingle.lrange(key,start,end).toString();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.lrange(key,start,end).toString();
		}else if(redisType.equals("2")){
			return jedisCluster.lrange(key,start,end).toString();
		}
		return null;
	}
	
	/**
	 * 获取list类型的所有数据
	 * @param key
	 * @return
	 */
	public List<String> lrange_all(String key){
		if(redisType.equals("0")){
			return jedisSingle.lrange(key,0,-1);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.lrange(key,0,-1);
		}else if(redisType.equals("2")){
			return jedisCluster.lrange(key,0,-1);
		}
		return null;
	}
	
	/**
	 * 在key对应list的头部添加字符串元素，返回1表示成功，0表示key存在且不是list类型
	 * @param key
	 * @param value
	 * @return
	 */
	public String lpush(String key,String value){
		if(redisType.equals("0")){
			return jedisSingle.lpush(key,value).toString();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.lpush(key,value).toString();
		}else if(redisType.equals("2")){
			return jedisCluster.lpush(key,value).toString();
		}
		return null;
	}
	
	/**
	 * 在key对应list的尾部添加字符串元素，返回1表示成功，0表示key存在且不是list类型
	 * @param key
	 * @param value
	 * @return
	 */
	public String rpush(String key,String value){
		if(redisType.equals("0")){
			return jedisSingle.rpush(key,value).toString();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.rpush(key,value).toString();
		}else if(redisType.equals("2")){
			return jedisCluster.rpush(key,value).toString();
		}
		return null;
	}
	
	/**
	 * 添加list中指定下标的元素值，成功返回1，key或者下标不存在返回错误
	 * @param key
	 * @param index
	 * @param value
	 * @return
	 */
	public String lset(String key,int index,String value){
		if(redisType.equals("0")){
			return jedisSingle.lset(key,index,value).toString();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.lset(key,index,value).toString();
		}else if(redisType.equals("2")){
			return jedisCluster.lset(key,index,value).toString();
		}
		return null;
	}
	
	/**
	 * 从key对应list中删除count个和value相同的元素。count为0时候删除全部
	 * @param key
	 * @param count
	 * @param value
	 * @return
	 */
	public String lrem(String key,int count,String value){
		if(redisType.equals("0")){
			return jedisSingle.lrem(key,count,value).toString();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.lrem(key,count,value).toString();
		}else if(redisType.equals("2")){
			return jedisCluster.lrem(key,count,value).toString();
		}
		return null;
	}
	
	/**
	 * 从list的头部删除元素，并返回删除元素。如果key对应list不存在或者是空返回nil，如果key对应值不是list返回错误
	 * @param key
	 * @return
	 */
	public String lpop(String key){
		if(redisType.equals("0")){
			return jedisSingle.lpop(key).toString();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.lpop(key).toString();
		}else if(redisType.equals("2")){
			return jedisCluster.lpop(key).toString();
		}
		return null;
	}
	
	/**
	 * 从list的尾部删除元素，并返回删除元素。如果key对应list不存在或者是空返回nil，如果key对应值不是list返回错误
	 * @param key
	 * @return
	 */
	public String rpop(String key){
		if(redisType.equals("0")){
			return jedisSingle.rpop(key).toString();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.rpop(key).toString();
		}else if(redisType.equals("2")){
			return jedisCluster.rpop(key).toString();
		}
		return null;
	}
	
	/**
	 * 查询key对应list的长度，key不存在返回0,如果key对应类型不是list返回错误
	 * @param key
	 * @return
	 */
	public Long llen(String key){//Map<String, String>
		if(redisType.equals("0")){
			return jedisSingle.llen(key);
		}else if(redisType.equals("1")){
			return jedisMasterSlave.llen(key);
		}else if(redisType.equals("2")){
			return jedisCluster.llen(key);
		}
		return null;
	}
	
	/**
	 * 关闭redis连接，集群不支持close方法
	 */
	public void close(){
		if(redisType.equals("0")){
			jedisSingle.close();
		}else if(redisType.equals("1")){
			jedisMasterSlave.close();
		}else if(redisType.equals("2")){
			try {
				jedisCluster.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 清除当前数据库所有的值
	 * @return
	 */
	public String flushdb(){
		if(redisType.equals("0")){
			return jedisSingle.flushDB();
		}else if(redisType.equals("1")){
			return jedisMasterSlave.flushDB();
		}/*else if(redisType.equals("2")){
			return jedisCluster.flushDB();
		}*/
		return null;
	}
	
}
