package redis.com.e_chinalife.rtcs_cd.redisListener;

import redis.com.e_chinalife.rtcs_cd.redisAPI.OperationRedis;

public class Redis {
	
	public static void main(String[] args) throws Exception {
		
		try{
			this_run(args);
		}catch (Exception e) {
			Thread.sleep(5000);
			this_run(args);
		}
     }
	
	public static void this_run(String[] args){
		OperationRedis aa = new OperationRedis();
		//System.out.println("--------------------------");
		RedisListener bb = new RedisListener();
 	   	aa.psubscribe(bb);
	}
	
}
