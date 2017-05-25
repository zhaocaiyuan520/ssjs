package redis.com.e_chinalife.rtcs_cd.redisListener;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import redis.clients.jedis.Transaction;
import redis.com.e_chinalife.rtcs_cd.redisAPI.OperationRedis;
import redis.com.e_chinalife.rtcs_cd.util.DesUtils;

public class RedisListener extends KeyExpiredListener
{

    private static final Log LOG = LogFactory.getLog( RedisListener.class );
    //redisAPI
    private OperationRedis or;
    //连接数据库
    private static Connection conn = null;
    
    private static PreparedStatement ps;
    
    private static String sql = "insert into Cloud_Message(MessageContent,ToUser,fromuser,channel) values(?,?,?,?)";
    //定义日志
    private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
    
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
    
    //private SimpleDateFormat inForceDate = new SimpleDateFormat("yyyy/MM/dd");
    //添加数据库用到的信息
    private String channel = "7ccf7652-38e9-4379-b5c9-3d3109f301a3";
    
    private String fromuser = "admin";
    //reids生成数据的前缀
    private String appl_count = "appl_count:";
    
    private String appl_sum = "appl_sum:";
    //连接数据库信息
    private String classForName = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    
    private String sqlPath = "jdbc:sqlserver://10.253.129.240:1433;databaseName=TestHekaton";
    
    private String sqlUser = "A80FC8D21280C7ADA1CE8DC018E7F524";//sa
    
    private String sqlPassword = "DC9F35E1E1DBD850B80B05F4BF928A84";//Clic1234
    
    public RedisListener(){
        or = new OperationRedis();
        try {
			Class.forName(classForName).newInstance();
			conn = DriverManager.getConnection(sqlPath, DesUtils.decrypt(sqlUser), DesUtils.decrypt(sqlPassword));
        } catch (InstantiationException e) {
			LOG.error(e);
		} catch (IllegalAccessException e) {
			LOG.error(e);
		} catch (ClassNotFoundException e) {
			LOG.error(e);
		}catch (SQLException e) {
			LOG.error(e);
		}catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    /**
     * lpush执行的方法
     */
	@Override
	public void lpush_method(String key) {
		//key里面有指定内容的处理
		if(key.contains("INSUR_APPL_REGIST-MIO_LOG")){
			try{
				insur_appl_regist_mio_log(key);
			}catch (Exception e) {
				LOG.error(e);
			}
		}
	}

	/**
	 * set执行的方法
	 */
	@Override
	public void set_method(String key) {
		//System.out.println(or.getString(key));
	}

	/**
	 * del执行的方法
	 */
	@Override
	public void del_method(String key) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public void hset_method(String key) {
		if(key.contains("this_appl")){
			//try{
				insur_appl_appl_state_new(key);
			/*}catch (Exception e) {
				LOG.error(e);
			}*/
		}
	}
	
	/**
	 * 操作数据库的方法
	 * @param key
	 */
	public void insur_appl_regist_mio_log(String key){
		//获取从表数据
		List<String> list_value = or.lrange_all(key);
		//设置失效时间（秒）
		or.expireKey(key, 864000);
		//获取从表数据个数并判断是否有数据
		int list_size = list_value.size();
		if(list_size>1){
			try {
				//List<String> list_value = or.getlist_all(key);
				//将两个表的数据拆分出来
				String INSUR_APPL_REGIST=list_value.get(0);//表名,APPL_NO,SALES_BRANCH_NO,SALES_CODE,HOLDER,commitTime
				String MIO_LOG=list_value.get(1);//表名,MIO_DATE,AMNT,commitTime
				Date date_commitTime=null;
				//判断先后顺序赋值，并获取commitTime
				if(INSUR_APPL_REGIST.contains("INSUR_APPL_REGIST")){
					INSUR_APPL_REGIST=list_value.get(0);
					MIO_LOG=list_value.get(1);
					date_commitTime=formatter.parse(INSUR_APPL_REGIST.split(",")[5]);
				}else{
					INSUR_APPL_REGIST=list_value.get(1);
					MIO_LOG=list_value.get(0);
					date_commitTime=formatter.parse(MIO_LOG.split(",")[3]);
				}
				//操作数据库数据
				String touser="";
				StringBuffer messagecontent=new StringBuffer();
				ps=conn.prepareStatement(sql);
				String[] INSUR_APPL_REGIST_values=INSUR_APPL_REGIST.split(",");
				String[] MIO_LOG_values=MIO_LOG.split(",");
				
				touser=INSUR_APPL_REGIST_values[2]+INSUR_APPL_REGIST_values[3];
				messagecontent.append("亲爱的伙伴：您的客户").append(INSUR_APPL_REGIST_values[4]).append("的保单")
				.append(INSUR_APPL_REGIST_values[1]).append("于").append(MIO_LOG_values[1]).append("新单暂收费成功，金额")
				.append(MIO_LOG_values[2]).append("元。");
				
				ps.setString(1, messagecontent.toString());
				ps.setString(2, touser);
				ps.setString(3, fromuser);
				ps.setString(4, channel);
				ps.executeUpdate();
				//删除已经处理的数据
				or.del(key);
				//算取本条数据处理时间，添加到日志中
				long committime = date_commitTime.getTime(); 
		        long clTime = new Date().getTime(); 
		        LOG.info("操作数据库记录从数据的处理时间："+(clTime-committime));
			} catch (SQLException e) {
				LOG.error(e);
			}catch (ParseException e) {
				LOG.error(e);
			}  
		}
	}
	
	/**
	 * 实时统计操作方法
	 * 能走到这个方法说明里面已经有数据
	 * @param key
	 */
	public void insur_appl_appl_state_new(String key){
		//获取操作的数据
		Map<String, String> maplist = this.or.hgetAll(key);
		int maplist_size = maplist.size();
		//将主表从表的数据拆分出来
		String insur_appl_value = (String)maplist.get("INSUR_APPL");
		String appl_state = (String)maplist.get("APPL_STATE");
		if (maplist_size > 1) {
			//获取当前日期
			String date = sdf.format(new Date());
			//拆分主表数据
			String[] insur_appl_list = insur_appl_value.split(",");
			//获取从表的list数据
			List<String> appl_state_value_list = or.lrange_all(appl_state);
			if(appl_state_value_list!=null){
				int appl_state_value_list_length = appl_state_value_list.size();
				
				//需要生成的key
				String appl_count_key = "";
		        String appl_sum_key = "";
		        //循环从表的list列表，进行操作
				for(int i=0;i<appl_state_value_list_length;i++){
					try {
					//拆分多个从表数据
					String[] appl_state_list = appl_state_value_list.get(i).split(",");
					//获取省份编号
					String brach_no=change_branch_No(insur_appl_list[1]);
					//判断缴费方式
					if("Y".equals(insur_appl_list[2])){
						//获取缴费年期
						int appl_state_year = Integer.parseInt(appl_state_list[2]);
						//判断缴费年期
						if(appl_state_year<5){
							appl_count_key = appl_count + brach_no + ":" + insur_appl_list[3] + ":fl:"+date;
					        appl_sum_key = appl_sum + brach_no + ":" + insur_appl_list[3] + ":fl:"+date;
						}else if(appl_state_year>=5&&appl_state_year<=9){
							appl_count_key = appl_count + brach_no + ":" + insur_appl_list[3] + ":fm:"+date;
					        appl_sum_key =appl_sum + brach_no + ":" + insur_appl_list[3] + ":fm:"+date;
						}else if(appl_state_year>=10){
							appl_count_key = appl_count + brach_no + ":" + insur_appl_list[3] + ":fh:"+date;
					        appl_sum_key = appl_sum + brach_no + ":" + insur_appl_list[3] + ":fh:"+date;
						}
					}else{
						appl_count_key = appl_count + brach_no + ":" + insur_appl_list[3] + ":d:"+date;
				        appl_sum_key = appl_sum + brach_no + ":" + insur_appl_list[3] + ":d:"+date;
					}
					//计算金额
					double add_sum = Double.parseDouble(appl_state_list[1]);
					String set_sum = or.get(appl_sum_key);
					if(set_sum!=null){
						add_sum = add_sum+Double.parseDouble(set_sum);
					}
					BigDecimal b = new BigDecimal(add_sum);  
					double c = b.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue(); 
					
					
					//计算处理时间保存到日志
					//Date in_force_date = inForceDate.parse(insur_appl_list[4]);
					Date in_force_date = formatter.parse(insur_appl_list[4]);
					Date end_date = formatter.parse(insur_appl_list[5]);
					long committime = end_date.getTime(); 
			        long clTime = new Date().getTime(); 
			        long czTime = clTime-committime;
			        LOG.info("appl记录从数据的处理时间："+czTime);
			        long interval = (in_force_date.getTime() - clTime)/1000;
			        //执行事务
					Transaction redis_Transaction=or.multi();
					redis_Transaction.rpop(appl_state);
					redis_Transaction.set(appl_sum_key, c+"");
					redis_Transaction.incr(appl_count_key);
					redis_Transaction.expire(appl_sum_key, 604800);
					redis_Transaction.expire(appl_count_key, 604800);
					redis_Transaction.exec();
					or.expireKey(key, new Long(interval).intValue());
				        
					} catch (ParseException e) {
						// TODO Auto-generated catch block
						LOG.error(e+";key="+key);
					}
				}
			}
		}
	}
	
	/**
	 * 用来构造branchNo机构号的方法 特殊五个用前4位加00，其余用前2位加0000
	 * 注意！目前只构建到省级，以后要是全部接入的话就要更改逻辑
	 * 
	 * @return
	 */
	public static String change_branch_No(String branchNo) {
		String result="";
			if (branchNo.substring(0, 4).equals("2102") || branchNo.substring(0, 4).equals("3302")
					|| branchNo.substring(0, 4).equals("3502") || branchNo.substring(0, 4).equals("3702")
					|| branchNo.substring(0, 4).equals("4402")) {
				result = new StringBuffer().append(branchNo.substring(0, 4)).append("00").toString();
			} else {
				result = new StringBuffer().append(branchNo.substring(0, 2)).append("0000").toString();
			}
		return result;
	}

	/**
	 * 重新启动执行方法，查找没有处理的数据
	 */
	@Override
	public void redis_initialization() {
		List<String> this_appl_list = or.getkeys("this_appl*");
		List<String> insur_appl_regist_mio_log_list = or.getkeys("INSUR_APPL_REGIST-MIO_LOG*");
		if(this_appl_list!=null){
			int this_appl_list_size = this_appl_list.size();
			for(int i=0;i<this_appl_list_size;i++){
				String thistype=or.type(this_appl_list.get(i));
				if("hash".equals(thistype)){
					hset_method(this_appl_list.get(i));
				}
			}
		}
		if(insur_appl_regist_mio_log_list!=null){
			int insur_appl_regist_mio_log_list_size = insur_appl_regist_mio_log_list.size();
			for(int i=0;i<insur_appl_regist_mio_log_list_size;i++){
				lpush_method(insur_appl_regist_mio_log_list.get(i));
			}
		}
		
	}
	
}
