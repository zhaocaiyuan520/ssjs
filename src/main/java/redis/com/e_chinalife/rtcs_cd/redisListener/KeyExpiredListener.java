package redis.com.e_chinalife.rtcs_cd.redisListener;

import redis.clients.jedis.JedisPubSub;

public abstract class KeyExpiredListener extends JedisPubSub
{

	//notify-keyspace-events "KEA"
	
	// 初始化按表达式的方式订阅时候的处理  
	public void onPSubscribe(String pattern, int subscribedChannels) {  
		redis_initialization();
	}  
	
    // 取得按表达式的方式订阅的消息后的处理 
    @Override
    public void onPMessage( String pattern, String channel, String message )
    {
        String ch = channel.split( ":" )[1];
       // LOG.warn("onPMessage pattern: " + pattern + " & " + channel + " & " + message);
        //判断操作类型
        if("lpush".equals(ch)){
        	//LOG.warn( "onPMessage pattern: " + pattern + " & " + channel + " & " + message );
        	lpush_method(message);
        }
        if("set".equals(ch)){
        	set_method(message);
        }
        if("del".equals(ch)){
        	del_method(message);
        }
        if("hset".equals(ch)){
        	hset_method(message);
        }
    }
    
	public abstract void lpush_method(String key );
	
	public abstract void set_method(String key );
	
	public abstract void del_method(String key );
	
	public abstract void hset_method(String key );
	
	public abstract void redis_initialization(); 
}
