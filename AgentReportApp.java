package controllers.wechat.report;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import controllers.sso.client.LoginUtil;
import groovy.sql.Sql;
import models.media.channel.ChannelPhoneService;
import models.organization.User;
import models.wechat.WechatSetting;
import play.data.validation.Required;
import play.modules.morphia.Model.MorphiaQuery;
import play.mvc.Controller;
import utils.DateUtil;
import utils.JdbcQuery;
import utils.JdbcQueryHelper;
import utils.PageResult;
import utils.QueryTermTuple;
import utils.StringUtil;

public class AgentReportApp extends Controller {
	/**
	 * 整体运营报表
	 * add by 2017-4-27
	 * */
	public static void wholeRunning(String dateType ,String startDate,String endDate,QueryTermTuple[] p,String channel, int page, int rows, String sort, String order) {
		WechatSetting w = WechatSetting.findById("T0000");
		String sql = "";
		boolean isLevelCode = false;   
		String levelString = "";
		List sdlist = new ArrayList();
	    if(p!=null){
	    	for(int i = 0;i<p.length;i++){
	    		if(p[i].fn!=null&&p[i].fn.equals("levelCode")&&p[i].fv!=null&&!p[i].fv.equals("")){
			    	isLevelCode =true;
			    	levelString =p[i].fv;
			    	p[i].fn = "";
	    			p[i].fv = "";
	    			p[i].lo = "";
			    	break;
	    		}
	    	}
	    }
		    //截取时间字段
		    sql = timeSub(sql,dateType);
		    
		    sql+= "t.channel channel, count(distinct t.open_id) customer_num,"
				+ " count(*) request_artificial,count(case when a.is_answer = '1' then 1 end) agent_answer_session_num,"
				+ " count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) effective_session_num,"
				+ " round((case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) > 0 "
				+ " then count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) "
				+ " /decode(count(case when a.is_answer = '1' then 1 end),0,1,count(case when a.is_answer = '1' then 1 end))*100 else 0 end),1) effective_session_rate,"
				+ " round(case when count(case when a.is_answer = '1' then 1 end) > 0 then"
				+ " count(case when ((to_date(a.first_agent_msg_time,'yyyy-mm-dd hh24-mi-ss') - "
				+ " to_date(a.answer_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60) < "+Integer.parseInt(w.timeResponse)+" then 1 end)/ "
				+ " decode(count(case when a.is_answer = '1' then 1 end),0,1,count(case when a.is_answer = '1' then 1 end))*100 else 0 end,1) time_response_rate,"
				+ "count(case when a.visitor_msg_count > 0 then 1 end) customer_effective_session_num,"
				+ " count(case when a.agent_msg_count > 0 then 1 end) agent_effective_session_num,"
				+ "nvl(sum(a.visitor_msg_count),0) customer_message_num, "
				+ "nvl(sum(a.agent_msg_count),0) agent_message_num,"
				+ "count(case when a.client_id is null then 1 end) call_lost_num,"
				+ "round(case when count(case when t.complet_mode = '2' then 1 end) = 0 then 0 "
				+ "else sum(case when t.complet_mode = '2' then ((to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60) else 0 end)/decode(count(case when t.complet_mode = '2' then 1 end),0,1,count(case when t.complet_mode = '2' then 1 end)) end,0) average_giveup_timelength"
				+ " from   ";
				if(isLevelCode){
					sql += " (select * from wechat_visitor where levelcode like ?)"; 
					sdlist.add(levelString+"%");
				}else{
					sql +=" wechat_visitor ";
				}
				
				sql+= " t left join wechat_answer a on t.client_id = a.client_id "
						+ "left join wechat_visitor_disconnect d on t.client_id = d.client_id "
				+ " where t.reconn = '0' and t.user_type = 'user' " ;
				//+ "and t.create_time between '2017-04-10' and '2017-05'";
		
		    if(startDate!=null&&!"".equals(startDate)){
				sql+=" and t.create_time >= ?";
				sdlist.add(startDate);
			}
			if(endDate!=null&&!"".equals(endDate)){
				sql+=" and t.create_time <= ?";
				sdlist.add(endDate);
			}
			if(channel!=null&&!"".equals(channel)&&(!"all".equals(channel))){
				sql+=" and t.channel = ?";
				sdlist.add(channel);
			}
			connect(sdlist, sql, p);
			JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(sql);
		//	JdbcQueryHelper.concatQueryByTermTuples(jdbcQuery, p);
			String sbf = " group by  ";
			//截取时间分组
			String parameter = "t.channel";
			sbf = timeSubstr(sbf,sort,dateType,order,parameter);
			
	        jdbcQuery = jdbcQuery.concat(sbf);
	        List<Map<String, Object>> list = jdbcQuery.asList(sdlist.toArray());
		//	List<Map<String, Object>> list = jdbcQuery.asList();
		renderJSON(PageResult.Page(list,page,rows));
	}	
	
	/**
	 *  业务分类报表
	 *  add-by 2017-05-08 lixiang
	 **/
	public static void businessClassify(String dateType ,String startDate,String endDate,QueryTermTuple[] p,String skillgroups, int page, int rows, String sort, String order) {
		String sql = "";
		//截取时间字段
	    sql = timeSub(sql,dateType);
		sql += " substr(t.skill,0,6) business_skill_group, count(distinct t.open_id) customer_num, count(*) request_artificial,"
				+" count(case when a.is_answer = '1' then 1 end) agent_answer_session_num, "
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) effective_session_num,"
				+" count(case when a.visitor_msg_count > 0 then 1 end) customer_effective_session_num, "
				+" count(case when a.agent_msg_count > 0 then 1 end) agent_effective_session_num,"
				+" count(case when (to_date(a.first_agent_msg_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 > 50 then 1 end) respond_num,"
				+" round(case when count(case when a.is_answer = '1' then 1 end) = 0 then 0 else"
				+" count(case when (to_date(a.first_agent_msg_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 > 50 then 1 end)/count(case when a.is_answer = '1' then 1 end)*100 end,2)||'%' response_ratio,"
				+" nvl(sum(a.visitor_msg_count),0) customer_message_num, nvl(sum(a.agent_msg_count),0) agent_message_num,"
				+" nvl(sum(case when a.create_time is not null then (to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 end),0) line_total_timelength,"
				+" round(case when count(*) = 0 then 0 else nvl(sum(case when a.create_time is not null then (to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 end),0)/count(*) end,0) average_line_total_timelength,"
				+" nvl(max(case when a.create_time is not null then (to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 end),0) max_line_timelength,"
				+" count(case when a.client_id is null then 1 end) call_lost_num,"
				+" round(case when count(case when a.client_id is null then 1 end) = 0 then 0 "
				+" else sum(case when a.client_id is null then ((to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60) else 0 end)/count(case when a.client_id is null then 1 end) end,0) average_giveup_timelength,"
				+" nvl(sum(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else 0 end),0) ef_session_total_timelength,"
				+" round(case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) = 0 then 0 "
				+" else nvl(sum(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else 0 end),0)/"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) end,0) ef_session_average_timelength,"
				+" '0' asignin_timelength, '0' online_timelength, '0' average_online_timelength, '0' busy_timelength, '0' average_busy_timelength"
				+" from wechat_biz t left join wechat_answer a on t.client_id = a.client_id "
				+" left join wechat_visitor_disconnect d on t.client_id = d.client_id"
		        +" where t.reconn = '0' ";
		List paList = new ArrayList();
		if(!StringUtil.isNullStr(startDate)){
			//sql+=" and t.create_time >=  '"+startDate+"'";
			sql+=" and t.create_time >=  ?";
			paList.add(startDate);
		}
		if(!StringUtil.isNullStr(endDate)){
//			sql+=" and t.create_time <= '"+endDate+"'";
			sql+=" and t.create_time <= ?";
			paList.add(endDate);
		}
		if(!StringUtil.isNullStr(skillgroups)){
//			sql+=" and t.skill = '"+skillgroups+"'";
			sql+=" and t.skill = ?";
			paList.add(skillgroups);
		}
		
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(sql);
		JdbcQueryHelper.concatQueryByTermTuples(jdbcQuery, p);
		String sbf = " group by  ";
		//截取时间分组
		String parameter = "substr(t.skill,0,6)";
		sbf = timeSubstr(sbf,sort,dateType,order,parameter);
       
		jdbcQuery = jdbcQuery.concat(sbf);
	    renderJSON(PageResult.Page(jdbcQuery.asList(paList.toArray()),page,rows));
	}
	
	/**
	 *  技能组统计报表
	 *  add-by 2017-05-08 lixiang
	 **/
	public static void skillReport(String dateType ,String startDate,String endDate,QueryTermTuple[] p,String skill, int page, int rows, String sort, String order) {
		
		boolean isLevelCode = false;   
		String levelString = "";
		List paList = new ArrayList();
	    if(p!=null){
	    	for(int i = 0;i<p.length;i++){
	    		if(p[i].fn!=null&&p[i].fn.equals("levelCode")&&p[i].fv!=null&&!p[i].fv.equals("")){
			    	isLevelCode =true;
			    	levelString =p[i].fv;
			    	p[i].fn = "";
	    			p[i].fv = "";
	    			p[i].lo = "";
			    	break;
	    		}
	    	}
	    }
		//截取时间字段
	    String sql = "";
	    sql = timeSub(sql,dateType);
		sql += " t.skill skill_group, count(distinct t.open_id) customer_num, count(*) request_artificial,"
				+" count(case when a.is_answer = '1' then 1 end) agent_answer_session_num, "
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) effective_session_num,"
				+" count(case when a.visitor_msg_count > 0 then 1 end) customer_effective_session_num, "
				+" count(case when a.agent_msg_count > 0 then 1 end) agent_effective_session_num,"
				+" count(case when (to_date(a.first_agent_msg_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 > 50 then 1 end) respond_num,"
				+" round(case when count(case when a.is_answer = '1' then 1 end) = 0 then 0 else"
				+" count(case when (to_date(a.first_agent_msg_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 > 50 then 1 end)/count(case when a.is_answer = '1' then 1 end)*100 end,2) response_ratio,"
				+" nvl(sum(a.visitor_msg_count),0) customer_message_num, nvl(sum(a.agent_msg_count),0) agent_message_num,"
				+" nvl(sum(case when a.create_time is not null then (to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 end),0) line_total_timelength,"
				+" round(case when count(*) = 0 then 0 else nvl(sum(case when a.create_time is not null then (to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 end),0)/count(*) end,0) average_line_total_timelength,"
				+" nvl(max(case when a.create_time is not null then (to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 end),0) max_line_timelength,"
				+" count(case when a.client_id is null then 1 end) call_lost_num,"
				+" round(case when count(case when a.client_id is null then 1 end) = 0 then 0 "
				+" else sum(case when a.client_id is null then ((to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60) else 0 end)/count(case when a.client_id is null then 1 end) end,0) average_giveup_timelength,"
				+" nvl(sum(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else 0 end),0) ef_session_total_timelength,"
				+" round(case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) = 0 then 0 "
				+" else nvl(sum(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else 0 end),0)/"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) end,0) ef_session_average_timelength,"
				+" '0' asignin_timelength, '0' online_timelength, '0' average_online_timelength, '0' busy_timelength, '0' average_busy_timelength"
				+" from ";
			if(isLevelCode){
				sql +=" (select b.* from wechat_biz b inner join wechat_visitor v on b.client_id=v.client_id and v.levelcode like ?) ";
				paList.add(levelString+"%");
			}else{
				sql += " wechat_biz ";
			}
				
			sql += " t left join wechat_answer a on t.client_id = a.client_id "
				+" left join wechat_visitor_disconnect d on t.client_id = d.client_id"
		        +" where t.reconn = '0' ";

		if(!StringUtil.isNullStr(startDate)){
//			sql+=" and t.create_time >=  '"+startDate+"'";
			sql+=" and t.create_time >= ?";
			paList.add(startDate);
		}
		if(!StringUtil.isNullStr(endDate)){
//			sql+=" and t.create_time <= '"+endDate+"'";
			sql+=" and t.create_time <= ?";
			paList.add(endDate);
		}
		if(!StringUtil.isNullStr(skill)){
//			sql+=" and t.skill = '"+skill+"'";
			sql+=" and t.skill = ?";
			paList.add(skill);
		}
		connect(paList, sql, p);
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(sql);
		//JdbcQueryHelper.concatQueryByTermTuples(jdbcQuery, p);
		String sbf = " group by  ";
		//截取时间分组
		String parameter = "t.skill";
		sbf = timeSubstr(sbf,sort,dateType,order,parameter);
       
		jdbcQuery = jdbcQuery.concat(sbf);
	    renderJSON(PageResult.Page(jdbcQuery.asList(paList.toArray()),page,rows));
	}
	
	/**
	 *  聊天入口报表
	 *  add-by 2017-05-08 lixiang
	 **/
	public static void chatInletReport(String dateType ,String startDate,String endDate,QueryTermTuple[] p,String chatInlet, int page, int rows, String sort, String order) {
		boolean isLevelCode = false;   
		String levelString = "";
		List paList = new ArrayList();
	    if(p!=null){
	    	for(int i = 0;i<p.length;i++){
	    		if(p[i].fn!=null&&p[i].fn.equals("levelCode")&&p[i].fv!=null&&!p[i].fv.equals("")){
			    	isLevelCode =true;
			    	levelString =p[i].fv;
			    	p[i].fn = "";
	    			p[i].fv = "";
	    			p[i].lo = "";
			    	break;
	    		}
	    	}
	    }
		String sql = "";
		//截取时间字段
	    sql = timeSub(sql,dateType);
		sql += " t.chat_inlet chat_inlet, count(distinct t.open_id) customer_num, count(*) request_artificial,"
				+" count(case when a.is_answer = '1' then 1 end) agent_answer_session_num, "
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) effective_session_num,"
				+" count(case when a.visitor_msg_count > 0 then 1 end) customer_effective_session_num, "
				+" count(case when a.agent_msg_count > 0 then 1 end) agent_effective_session_num,"
				+" count(case when (to_date(a.first_agent_msg_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 > 50 then 1 end) respond_num,"
				+" round(case when count(case when a.is_answer = '1' then 1 end) = 0 then 0 else"
				+" count(case when (to_date(a.first_agent_msg_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 > 50 then 1 end)/count(case when a.is_answer = '1' then 1 end)*100 end,2) response_ratio,"
				+" nvl(sum(a.visitor_msg_count),0) customer_message_num, nvl(sum(a.agent_msg_count),0) agent_message_num,"
				+" nvl(sum(case when a.create_time is not null then (to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 end),0) line_total_timelength,"
				+" round(case when count(*) = 0 then 0 else nvl(sum(case when a.create_time is not null then (to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 end),0)/count(*) end,0) average_line_total_timelength,"
				+" nvl(max(case when a.create_time is not null then (to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 end),0) max_line_timelength,"
				+" count(case when a.client_id is null then 1 end) call_lost_num,"
				+" round(case when count(case when a.client_id is null then 1 end) = 0 then 0 "
				+" else sum(case when a.client_id is null then ((to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60) else 0 end)/count(case when a.client_id is null then 1 end) end,0) average_giveup_timelength,"
				+" nvl(sum(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else 0 end),0) ef_session_total_timelength,"
				+" round(case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) = 0 then 0 "
				+" else nvl(sum(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(a.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else 0 end),0)/"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) end,0) ef_session_average_timelength"
				+" from ";
		if(isLevelCode){
			sql+= " (select * from wechat_visitor where levelcode like ?) ";
			paList.add(levelString+"%");
		}else{
			sql+= " wechat_visitor ";
		}
				
		sql += " t left join wechat_answer a on t.client_id = a.client_id "
				+" left join wechat_visitor_disconnect d on t.client_id = d.client_id"
		        +" where t.reconn = '0' and t.user_type = 'user' ";
		if(!StringUtil.isNullStr(startDate)){
//			sql+=" and t.create_time >=  '"+startDate+"'";
			sql+=" and t.create_time >=  ?";
			paList.add(startDate);
		}
		if(!StringUtil.isNullStr(endDate)){
//			sql+=" and t.create_time <= '"+endDate+"'";
			sql+=" and t.create_time <= ?";
			paList.add(endDate);
		}
		if(!StringUtil.isNullStr(chatInlet)){
//			sql+=" and t.chat_inlet = '"+chatInlet+"'";
			sql+=" and t.chat_inlet = ?";
			paList.add(chatInlet);
		}
		connect(paList, sql, p);
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(sql);
//      JdbcQueryHelper.concatQueryByTermTuples(jdbcQuery, p);
		String sbf = " group by  ";
		//截取时间分组
		String parameter = "t.chat_inlet";
		sbf = timeSubstr(sbf,sort,dateType,order,parameter);
       
		System.out.println("sql-------------: "+sql);
		jdbcQuery = jdbcQuery.concat(sbf);
	    renderJSON(PageResult.Page(jdbcQuery.asList(paList.toArray()),page,rows));
	}
	
	/**
	 *  国税聊天入口技能组交叉报表
	 *  add-by 2017-07-25 sunsy
	 **/
	public static void chatInletSkillReport(String dateType ,String startDate,String endDate,QueryTermTuple[] p,String chatInlet, int page, int rows, String sort, String order) {
		String sql = "";
		String[] array = new String[]{"100100","100200","100300","100400","100500"};
		sql += " with t as (select v.client_id,v.create_time,v.chat_inlet,b.skill from wechat_visitor v "
		+" left join wechat_biz b on v.client_id = b.client_id where v.user_type = 'user'";
		List paList = new ArrayList();
		if(!StringUtil.isNullStr(startDate)){
			//sql+=" and v.create_time >=  '"+startDate+"'";
			sql+=" and v.create_time >=  ?";
			paList.add(startDate);
		}
		if(!StringUtil.isNullStr(endDate)){
			//sql+=" and v.create_time <= '"+endDate+"'";
			sql+=" and v.create_time <= ?"; 
			paList.add(endDate);
		}
		if(!StringUtil.isNullStr(chatInlet)){
			//sql+=" and v.chat_inlet = '"+chatInlet+"'";
			sql+=" and v.chat_inlet = ?";
			paList.add(chatInlet);
		}
		sql += ") select d.*,";
		for(int i=0;i<array.length;i++){
			sql += " round(case when e.skill_"+array[i]+"_all_total = 0 then 0 else d.skill_"+array[i]+"_total/e.skill_"+array[i]+"_all_total end,2)*100||'%' skill_"+array[i]+"_inletper,";
		}
		sql += " '' from( select ";
	    sql = timeSub(sql,dateType);//截取时间字段
		sql += " t.chat_inlet chat_inlet,  ";
		for(int i=0;i<array.length;i++){
			sql += " count(case when substr(t.skill,0,6) = '"+array[i]+"' then 1 end) skill_"+array[i]+"_total, "
			+" count(case when substr(t.skill,0,6) = '"+array[i]+"' and a.id is not null then 1 end) skill_"+array[i]+"_answer, "
			+" count(case when substr(t.skill,0,6) = '"+array[i]+"' and a.id is null then 1 end) skill_"+array[i]+"_calllost, "
			+" round(case when count(case when substr(t.skill,0,6) = '"+array[i]+"' then 1 end) = 0 then 0 else count(case when substr(t.skill,0,6) = '"+array[i]+"'  "
			+" and a.id is not null then 1 end)/count(case when substr(t.skill,0,6) = '"+array[i]+"' then 1 end) end,2)*100||'%' skill_"+array[i]+"_answerper, ";
		}
		sql += " '' from t left join wechat_answer a on t.client_id = a.client_id  where a.reconn = '0' ";
//		JdbcQueryHelper.concatQueryByTermTuples(jdbcQuery, p);
		String timeRange = " substr(t.create_time,0,13) ";
		if("date".equals(dateType)) {
  			timeRange = " substr(t.create_time,0,10) ";
  		}else if("month".equals(dateType)) {
  			timeRange = " substr(t.create_time,0,7) ";
  		}else if("year".equals(dateType)) {
  			timeRange = " substr(t.create_time,0,4) ";
  		}
		sql += " group by "+timeRange+", t.chat_inlet ";
//		sql += " having t.chat_inlet is not null ";
		sql += ") d left join (select "+timeRange+" time_range,count(*) total,";
		for(int i=0;i<array.length;i++){
			sql += " count(case when substr(t.skill,0,6) = '"+array[i]+"' then 1 end) skill_"+array[i]+"_all_total,";
		}
		sql += " '' from t group by "+timeRange+") e on d.time_range = e.time_range "
			+" order by d.time_range desc";
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().concat(sql);
	    renderJSON(PageResult.Page(jdbcQuery.asList(paList.toArray()),page,rows));
	}
	
	/**
	 *  整体满意度报表
	 *  add-by 2017-05-10 lixiang
	 **/
	public static void wholeSatisfy(String dateType ,String startDate,String endDate,QueryTermTuple[] p,String chatInlet,String channel, int page, int rows, String sort, String order) {
		boolean isLevelCode = false;   
		String levelString = "";
		List paList = new ArrayList();
	    if(p!=null){
	    	for(int i = 0;i<p.length;i++){
	    		if(p[i].fn!=null&&p[i].fn.equals("levelCode")&&p[i].fv!=null&&!p[i].fv.equals("")){
			    	isLevelCode =true;
			    	levelString =p[i].fv;
			    	p[i].fn = "";
	    			p[i].fv = "";
	    			p[i].lo = "";
			    	break;
	    		}
	    	}
	    }
		String sql = "";
		//截取时间字段
	    sql = timeSub(sql,dateType);
		/*sql += " count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) effective_session_num,"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end) evaluate_num,"
				+" round(case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) = 0 then 0 else"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end)"
				+" /count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end)*100 end,2)||'%' evaluate_ratio,"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is null then 1 end) not_evaluate_num,"
				+" round(case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) = 0 then 0 else"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is null then 1 end)"
				+" /count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end)*100 end,2)||'%' not_evaluate_ratio,"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and (s.result = '5' or s.result = '4') then 1 end) very_satisfied_num,"
				+" round(case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end) = 0 then 0 else"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and (s.result = '5' or s.result = '4') then 1 end)"
				+" /count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end)*100 end,2)||'%' very_satisfied_ratio,"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and (s.result = '3' or s.result = '2') then 1 end) satisfied_num,"
				+" round(case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end) = 0 then 0 else"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and (s.result = '3' or s.result = '2') then 1 end)"
				+" /count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end)*100 end,2)||'%' satisfied_ratio,"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.result = '1' then 1 end) not_satisfied_num,"
				+" round(case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end) = 0 then 0 else"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.result = '1' then 1 end)"
				+" /count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end)*100 end,2)||'%' not_satisfied_ratio"
				+" from wechat_visitor t left join wechat_answer a on t.client_id = a.client_id "
				+" left join wechat_satisfaction s on t.client_id = s.customer_session_id "
		        +" where t.reconn = '0' and t.user_type = 'user' and a.id is not null";*/
	    sql += " count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) effective_session_num,"
				+" count(case when  s.customer_session_id is not null then 1 end) evaluate_num,"
				+" round(case when count(*) = 0 then 0 else"
				+" count(case when s.customer_session_id is not null then 1 end)"
				+" /count(*)*100 end,2) evaluate_ratio,"
				+" count(case when s.customer_session_id is null then 1 end) not_evaluate_num,"
				+" round(case when count(*) = 0 then 0 else"
				+" count(case when s.customer_session_id is null then 1 end)"
				+" /count(*)*100 end,2) not_evaluate_ratio,"
				+" count(case when (s.result = '1') then 1 end) very_satisfied_num,"
				+" round(case when count(case when s.customer_session_id is not null then 1 end) = 0 then 0 else"
				+" count(case when (s.result = '1') then 1 end)"
				+" /count(case when s.customer_session_id is not null then 1 end)*100 end,2) very_satisfied_ratio,"
				+" count(case when (s.result = '2') then 1 end) satisfied_num,"
				+" round(case when count(case when s.customer_session_id is not null then 1 end) = 0 then 0 else"
				+" count(case when (s.result = '2') then 1 end)"
				+" /count(case when s.customer_session_id is not null then 1 end)*100 end,2) satisfied_ratio,"
				+" count(case when s.result >= '3' then 1 end) not_satisfied_num,"
				+" round(case when count(case when s.customer_session_id is not null then 1 end) = 0 then 0 else"
				+" count(case when s.result >= '3' then 1 end)"
				+" /count(case when s.customer_session_id is not null then 1 end)*100 end,2) not_satisfied_ratio"
				+" from ";
	    if(isLevelCode){
	    	sql+= " (select * from wechat_visitor where levelcode like ?) ";
			paList.add(levelString+"%");
	    }else{
	    	sql	+= " wechat_visitor ";
	    }
			sql	+= " t left join wechat_answer a on t.client_id = a.client_id "
				+" left join wechat_satisfaction s on t.client_id = s.customer_session_id "
		        +" where t.reconn = '0' and t.user_type = 'user' and a.id is not null";
		if(!StringUtil.isNullStr(startDate)){
			//sql+=" and t.create_time >=  '"+startDate+"'";
			sql+=" and t.create_time >=  ?";
			paList.add(startDate);
		}
		if(!StringUtil.isNullStr(endDate)){
			//sql+=" and t.create_time <= '"+endDate+"'";
			sql+=" and t.create_time <= ?";
			paList.add(endDate);
		}
		if(!StringUtil.isNullStr(chatInlet)){
			//sql+=" and t.chat_inlet = '"+chatInlet+"'";
			sql+=" and t.chat_inlet = ?";
			paList.add(chatInlet);
		}
		if(!StringUtil.isNullStr(channel)&&(!channel.equals("all"))){
			//sql+=" and t.channel = '"+channel+"'";
			sql+=" and t.channel = ?";
			paList.add(channel);
		}
		connect(paList, sql, p);
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(sql);
		//JdbcQueryHelper.concatQueryByTermTuples(jdbcQuery, p);
		String sbf = " group by  ";
		
		if(StringUtil.isNullStr(sort)){
		    if(null !=dateType && !"".equals(dateType)){
		    	  if("time".equals(dateType)) {
		    		  sbf +=" substr(t.create_time,0,13) "; 
		    		  sbf +=" order by substr(t.create_time,0,13) desc ";  
		    		 }else if("date".equals(dateType)) { 
		    			 sbf +=" substr(t.create_time,0,10) ";  
		    			 sbf +=" order by substr(t.create_time,0,10) desc ";
		    		  }else if("month".equals(dateType)) {
		    			  sbf +=" substr(t.create_time,0,7) ";  
		    			  sbf +=" order by substr(t.create_time,0,7) desc ";
		    		 }else if("year".equals(dateType)) {
		    			 sbf +=" substr(t.create_time,0,4) "; 
		    			 sbf +=" order by substr(t.create_time,0,4) desc ";
		    		}		    	 
		    }else{
		    	 sbf +=" substr(t.create_time,0,13) ";  
		    	 sbf +=" order by substr(t.create_time,0,13) desc ";
		    }
		}
		else{
			sbf = sbf + "ORDER BY "+sort+" "+order;
		}
       
		jdbcQuery = jdbcQuery.concat(sbf);
	    renderJSON(PageResult.Page(jdbcQuery.asList(paList.toArray()),page,rows));
	}
	
	/**
	 *  坐席满意度评价报表
	 *  add-by 2017-05-10 lixiang
	 **/
	public static void seatSatisfy(String dateType ,String startDate,String endDate,QueryTermTuple[] p,String agentId, int page, int rows, String sort, String order) {
		boolean isLevelCode = false;   
		String levelString = "";
		List paList = new ArrayList();
	    if(p!=null){
	    	for(int i = 0;i<p.length;i++){
	    		if(p[i].fn!=null&&p[i].fn.equals("levelCode")&&p[i].fv!=null&&!p[i].fv.equals("")){
			    	isLevelCode =true;
			    	levelString =p[i].fv;
			    	p[i].fn = "";
	    			p[i].fv = "";
	    			p[i].lo = "";
			    	break;
	    		}
	    	}
	    }
		String sql = "";
		//截取时间字段
	    sql = timeSub(sql,dateType);
		/*sql += " a.agent_id agent_dn,"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) effective_session_num,"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end) evaluate_num,"
				+" round(case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) = 0 then 0 else"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end)"
				+" /count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end)*100 end,2)||'%' evaluate_ratio,"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is null then 1 end) not_evaluate_num,"
				+" round(case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) = 0 then 0 else"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is null then 1 end)"
				+" /count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end)*100 end,2)||'%' not_evaluate_ratio,"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and (s.result = '5' or s.result = '4') then 1 end) very_satisfied_num,"
				+" round(case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end) = 0 then 0 else"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and (s.result = '5' or s.result = '4') then 1 end)"
				+" /count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end)*100 end,2)||'%' very_satisfied_ratio,"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and (s.result = '3' or s.result = '2') then 1 end) satisfied_num,"
				+" round(case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end) = 0 then 0 else"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and (s.result = '3' or s.result = '2') then 1 end)"
				+" /count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end)*100 end,2)||'%' satisfied_ratio,"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.result = '1' then 1 end) not_satisfied_num,"
				+" round(case when count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end) = 0 then 0 else"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.result = '1' then 1 end)"
				+" /count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 and s.customer_session_id is not null then 1 end)*100 end,2)||'%' not_satisfied_ratio"
				+" from wechat_visitor t left join wechat_answer a on t.client_id = a.client_id "
				+" left join wechat_satisfaction s on t.client_id = s.customer_session_id "
		        +" where t.reconn = '0' and t.user_type = 'user' and a.id is not null ";*/
	    sql += " a.agent_id agent_dn,"
				+" count(case when a.visitor_msg_count > 0 and a.agent_msg_count > 0 then 1 end) effective_session_num,"
				+" count(case when s.customer_session_id is not null then 1 end) evaluate_num,"
				+" round(case when count(*) = 0 then 0 else"
				+" count(case when s.customer_session_id is not null then 1 end)"
				+" /count(*)*100 end,2) evaluate_ratio,"
				+" count(case when s.customer_session_id is null then 1 end) not_evaluate_num,"
				+" round(case when count(*) = 0 then 0 else"
				+" count(case when s.customer_session_id is null then 1 end)"
				+" /count(*)*100 end,2) not_evaluate_ratio,"
				+" count(case when (s.result = '1') then 1 end) very_satisfied_num,"
				+" round(case when count(case when s.customer_session_id is not null then 1 end) = 0 then 0 else"
				+" count(case when (s.result = '1') then 1 end)"
				+" /count(case when s.customer_session_id is not null then 1 end)*100 end,2) very_satisfied_ratio,"
				+" count(case when (s.result = '2') then 1 end) satisfied_num,"
				+" round(case when count(case when s.customer_session_id is not null then 1 end) = 0 then 0 else"
				+" count(case when (s.result = '2') then 1 end)"
				+" /count(case when s.customer_session_id is not null then 1 end)*100 end,2) satisfied_ratio,"
				+" count(case when s.result >= '3' then 1 end) not_satisfied_num,"
				+" round(case when count(case when s.customer_session_id is not null then 1 end) = 0 then 0 else"
				+" count(case when s.result >= '3' then 1 end)"
				+" /count(case when s.customer_session_id is not null then 1 end)*100 end,2) not_satisfied_ratio"
				+" from ";
		 if(isLevelCode){
		    	sql+= " (select * from wechat_visitor where levelcode like ?) ";
				paList.add(levelString+"%");
		    }else{
		    	sql	+= " wechat_visitor ";
		    }
			sql	+= " t left join wechat_answer a on t.client_id = a.client_id "
				+" left join wechat_satisfaction s on t.client_id = s.customer_session_id "
		        +" where t.reconn = '0' and t.user_type = 'user' and a.id is not null ";
		if(!StringUtil.isNullStr(startDate)){
			//sql+=" and t.create_time >=  '"+startDate+"'";
			sql+=" and t.create_time >=  ?";
			paList.add(startDate);
		}
		if(!StringUtil.isNullStr(endDate)){
			//sql+=" and t.create_time <= '"+endDate+"'";
			sql+=" and t.create_time <=  ?";
			paList.add(endDate);
		}
		if(!StringUtil.isNullStr(agentId)){
			MorphiaQuery q =  User.createQuery();
			/*q.criteria("userName").equal(p[i].fv);*/
			q.field("userName").contains(agentId);
			List<User> list = q.asList();
			//agentId = "('"+agentId+"'";
			paList.add(agentId);
			agentId= "(?";
			if(list != null && list.size()>0){
				for (int i = 0; i < list.size(); i++) {
					if(list.size()<=1){
						agentId = list.get(i).userId;
						sql+=" and a.agent_id = '"+agentId+"'";
						paList.remove(paList.size()-1);
					}else{
						agentId = agentId + ",'" + list.get(i).userId+"'";
					}
				}
				if(list.size()>1){
					sql+=" and a.agent_id in "+agentId+")";
				}
			}else{
				sql+=" and a.agent_id = null";
			}
			
		}
		connect(paList, sql, p); 
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(sql);
		//JdbcQueryHelper.concatQueryByTermTuples(jdbcQuery, p);
		String sbf = " group by  ";
		//截取时间分组
		String parameter = "a.agent_id";
		sbf = timeSubstr(sbf,sort,dateType,order,parameter);
       
		jdbcQuery = jdbcQuery.concat(sbf);
	    renderJSON(PageResult.Page(jdbcQuery.asList(paList.toArray()),page,rows));
	}
	
	public static void serviceInfo(String timedates,String timedatee,String type1,String type2,String skill,String channel,String type3,String type4,String type5,String nickname,QueryTermTuple[] p,String agentId, int page, int rows, String sort, String order){
		User user = LoginUtil.getCurrentUser();
		
		String where = " where 1=1 ";
		
		if(StringUtil.isNullStr(timedates)){
			timedates = DateUtil.getCurrentDateShortStyle() + " 00:00:00";
		}
		
		if(StringUtil.isNullStr(timedatee)){
			timedatee = DateUtil.getCurrentDateShortStyle() + " 23:59:59";
		}
		where += " and t.create_time >= '"+timedates+"' and t.create_time <= '"+timedatee+"'";
		if(!StringUtil.isNullStr(nickname)){
			where += " and nickname like '%"+nickname+"%' ";
		}
		if(!StringUtil.isNullStr(type1)){
			where += " and bizz_brand='"+type1+"' ";
		}
		if(!StringUtil.isNullStr(type2)){
			where += " and bizz_type2='"+type2+"' ";
		}
		if(!StringUtil.isNullStr(type3)){
			where += " and bizz_amenu='"+type3+"' ";
		}
		if(!StringUtil.isNullStr(type4)){
			where += " and bizz_bmenu='"+type4+"' ";
		}
		if(!StringUtil.isNullStr(type5)){
			where += " and end_func='"+type5+"' ";
		}
		if(!StringUtil.isNullStr(agentId)){
			where += " and creator like '%"+agentId+"%' ";
		}
		if(!StringUtil.isNullStr(skill)){
			where += " and client_skill_id='"+skill+"' ";
		}
		if(!StringUtil.isNullStr(channel) && !"all".equals(channel)){
			where += " and b.channel='"+channel+"' ";
		}
		String sql = " t.customer_id,customer_session_id,service_id,t.levelcode,bizz_usernumber,updateuser,lastupdatetime,client_skill_id,t.create_time,b.channel,hdt_s_primekey,creator,bizz_type,"
					+"nickname,bizz_brand,bizz_amenu,bizz_bmenu,bizz_type2,end_func,remark "
					+"from cc_channel_phone_service t join wechat_visitor b on t.customer_session_id=b.client_id left join wechat_wx_customer c on c.open_id=t.customer_id " + where;
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(sql);
		JdbcQueryHelper.concatQueryByTermTuples(jdbcQuery, p);
		jdbcQuery.concat(" and t.levelcode like '"+user.levelCode+"%'");
		jdbcQuery.concat(" order by t.create_time desc");
		
		System.out.println("服务小结的SQL："+jdbcQuery.sql);
		renderJSON(PageResult.Page(jdbcQuery.asList(),page,rows));
	}
	
	public static void businessCount(int dateType,String startDate,String endDate,String type1,String type2,QueryTermTuple[] p,String agentId, int page, int rows, String sort, String order){
		String where = "";
		if(startDate != null && endDate !=null && !"".equals(startDate) && !"".equals(endDate)){
			if("time".equals(dateType)){
				where  = " where create_time >='"+startDate+"' and create_time <='"+endDate+"'";
			}
			if("date".equals(dateType)){
				where  = " where substr(create_time,0,10) >='"+startDate+"' and substr(create_time,0,10) <='"+endDate+"'";
			}
			if("month".equals(dateType)){
				where  = " where substr(create_time,0,7) >='"+startDate+"' and substr(create_time,0,7) <'"+endDate+"'";
			}
			if("year".equals(dateType)){
				where  = " where substr(create_time,0,5) >='"+startDate+"' and substr(create_time,0,5) <'"+endDate+"'";
			}
		}
		
		String sql = " service_id,create_time,hdt_s_primekey,creator,bizz_type,(select nickname from wechat_wx_customer where open_id=t.customer_id) nickname,bizz_brand,bizz_amenu,bizz_bmenu,bizz_type2,end_func,remark from cc_channel_phone_service t "+where+" order by create_time desc";
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(sql);
		renderJSON(PageResult.Page(jdbcQuery.asList(),page,rows));
	}
	
	public static void updateServiceInfo(ChannelPhoneService prime){
		prime.setLastupdatetime(DateUtil.getCurrentTime());
		prime.setUpdateuser(LoginUtil.getCurrentUserName());
		prime.save();
	}
	
	
	/**
	 *  坐席会话报表
	 *  add-by 2017-05-10 lixiang
	 **/
	public static void conversate(String dateType ,String startDate,String endDate,QueryTermTuple[] p,String agentId, int page, int rows, String sort, String order) {
		boolean isLevelCode = false;   
		String levelString = "";
		List paList = new ArrayList();
	    if(p!=null){
	    	for(int i = 0;i<p.length;i++){
	    		if(p[i].fn!=null&&p[i].fn.equals("levelCode")&&p[i].fv!=null&&!p[i].fv.equals("")){
			    	isLevelCode =true;
			    	levelString =p[i].fv;
			    	p[i].fn = "";
	    			p[i].fv = "";
	    			p[i].lo = "";
			    	break;
	    		}
	    	}
	    }
		String sql = "";
		//截取时间字段
	    sql = timeSub(sql,dateType);
		sql += " t.agent_id agent_dn, count(distinct t.open_id) customer_num,"
				+" count(case when t.is_answer = '1' then 1 end) agent_answer_session_num, "
				+" count(case when t.visitor_msg_count > 0 and t.agent_msg_count > 0 then 1 end) effective_session_num,"
				+" round(case when count(case when t.is_answer = '1' then 1 end) > 0 then "
				+" (count(case when t.visitor_msg_count > 0 and t.agent_msg_count > 0 then 1 end)/"
				+" count(case when t.is_answer = '1' then 1 end))*100 else 0 end,1) effective_session_rate,"
				+" count(case when t.visitor_msg_count > 0 then 1 end) customer_effective_session_num, "
				+" count(case when t.agent_msg_count > 0 then 1 end) agent_effective_session_num,"
				+" count(case when (to_date(t.first_agent_msg_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 > 50 then 1 end) respond_num,"
				+" round(case when count(case when t.is_answer = '1' then 1 end) = 0 then 0 else"
				+" count(case when (to_date(t.first_agent_msg_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 > 50 then 1 end)/count(case when t.is_answer = '1' then 1 end)*100 end,2) response_ratio,"
				+" round(case when count(case when t.is_answer = '1' then 1 end) = 0 then 0 else "
				+" count(case when (to_date(t.first_agent_msg_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 < 50 then 1 end)/ "
				+" count(case when t.is_answer = '1' then 1 end)*100 end,2) time_response_ratio, "
				+" nvl(sum(t.visitor_msg_count),0) customer_message_num, nvl(sum(t.agent_msg_count),0) agent_message_num,"
				+" nvl(sum(case when t.visitor_msg_count > 0 and t.agent_msg_count > 0 then (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else 0 end),0) ef_session_total_timelength,"
				+" round(case when count(case when t.visitor_msg_count > 0 and t.agent_msg_count > 0 then 1 end) = 0 then 0 "
				+" else nvl(sum(case when t.visitor_msg_count > 0 and t.agent_msg_count > 0 then (to_date(d.disconnect_time,'yyyy-mm-dd hh24-mi-ss') - "
				+" to_date(t.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else 0 end),0)/"
				+" count(case when t.visitor_msg_count > 0 and t.agent_msg_count > 0 then 1 end) end,0) ef_session_average_timelength,"
				+" max(c.login_time) first_asignin_time, max(c.logout_time) last_asignout_time,"
				+" max(c.login_timelength) asignin_timelength, max(s.idle_timelength) online_timelength, max(s.busy_timelength) busy_timelength, "
				+" max(s.rest_timelength) rest_timelength, max(s.rest_num) rest_num,max(s.eat_timelength) eat_timelength "
				+" from ";
				//+" wechat_answer "
			if(isLevelCode){
				sql +=" (select b.* from wechat_answer b inner join wechat_visitor v on b.client_id=v.client_id and v.levelcode like ?) ";
				paList.add(levelString+"%");
			}else{
				sql += " wechat_answer ";
			}
			sql	+= " t left join wechat_visitor_disconnect d on t.client_id = d.client_id "
				+" left join ("
				+" with c as"
				+" (select c.agent_id, c.action, c.op_time, lead(c.op_time,1,0) over(partition by c.login_id order by c.op_time) next_time"
				+" from wechat_agent_action c "
				+" where (c.action = 'Login' or c.action = 'Logout') "; //时间条件统一
		
		
		if(!StringUtil.isNullStr(startDate)){
			sql+=" and c.op_time >=  '"+startDate+"'";
		}
		if(!StringUtil.isNullStr(endDate)){
			sql+=" and c.op_time <= '"+endDate+"'";
		}
		
		sql += ") select c.agent_id, ";
		
		if(null !=dateType && !"".equals(dateType)){
	    	  if("time".equals(dateType)) {
	    		  sql+="substr(c.op_time,0,13) " ;
	    		 }else if("date".equals(dateType)) { 
	    			 sql+="substr(c.op_time,0,10) " ;
	    		  }else if("month".equals(dateType)) {
	    			  sql+="substr(c.op_time,0,7) " ; 		 
	    		 }else if("year".equals(dateType)) {
	    			 sql+="substr(c.op_time,0,4) " ; 	       
	    		}		    	 
	    }else{
	    	 sql+="substr(c.op_time,0,13) " ;
	    }
		
		sql += "g_time, min(case when c.action = 'Login' then c.op_time end) login_time,"
				+" max(case when c.action = 'Logout' then c.op_time end) logout_time, "
				+" sum(case when c.next_time <> '0' and c.action = 'Login' then (to_date(c.next_time,'yyyy-mm-dd hh24-mi-ss') - to_date(c.op_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else 0 end) login_timelength"
				+" from c group by c.agent_id,  ";//分组条件统一
		
		String nx = "";
		if(null !=dateType && !"".equals(dateType)){
	    	  if("time".equals(dateType)) {
	    		  nx = "13";
	    		  sql+="substr(c.op_time,0,13) " ;
	    		 }else if("date".equals(dateType)) {
	    			 nx = "10";
	    			 sql+="substr(c.op_time,0,10) " ;
	    		  }else if("month".equals(dateType)) {
	    			  nx = "7";
	    			  sql+="substr(c.op_time,0,7) " ; 		 
	    		 }else if("year".equals(dateType)) {
	    			 nx = "4";
	    			 sql+="substr(c.op_time,0,4) " ; 	       
	    		}		    	 
	    }else{
	    	nx = "13";
	    	 sql+="substr(c.op_time,0,13) " ;
	    }
		
		sql+= " ) c on t.agent_id = c.agent_id and substr(t.create_time,0,"+nx+") = c.g_time"
				+" left join ("
				+" with s as"
				+" (select s.user_id, s.user_session_id, s.status, s.create_time,"
				+" lead(s.create_time,1,0) over(partition by s.user_session_id order by s.create_time) next_time"
				+" from wechat_status s ";//时间条件统一
		
		if(!StringUtil.isNullStr(startDate)){
			sql+=" where s.create_time >=  '"+startDate+"'";
		}
		if(!StringUtil.isNullStr(endDate)){
			sql+=" and s.create_time <= '"+endDate+"'";
		}
		
		sql+= " ) select s.user_id, ";
		
		if(null !=dateType && !"".equals(dateType)){
	    	  if("time".equals(dateType)) {
	    		  sql+="substr(s.create_time,0,13) g_time, " ;
	    		 }else if("date".equals(dateType)) { 
	    			 sql+="substr(s.create_time,0,10) g_time, " ;
	    		  }else if("month".equals(dateType)) {
	    			  sql+="substr(s.create_time,0,7) g_time, " ; 		 
	    		 }else if("year".equals(dateType)) {
	    			 sql+="substr(s.create_time,0,4) g_time, " ; 	       
	    		}		    	 
	    }else{
	    	 sql+="substr(s.create_time,0,13) g_time, " ;
	    }
		
		sql+= " sum(case when s.next_time <> '0' and (s.status = '5' or s.status = '1') then (to_date(s.next_time,'yyyy-mm-dd hh24-mi-ss') - to_date(s.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else 0 end) idle_timelength,"
				+" sum(case when s.next_time <> '0' and s.status = '3' then (to_date(s.next_time,'yyyy-mm-dd hh24-mi-ss') - to_date(s.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else 0 end) busy_timelength,"
				+" sum(case when s.next_time <> '0' and s.status = '2' then (to_date(s.next_time,'yyyy-mm-dd hh24-mi-ss') - to_date(s.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else 0 end) eat_timelength, "
				+" sum(case when s.next_time <> '0' and s.status = '4' then (to_date(s.next_time,'yyyy-mm-dd hh24-mi-ss') - to_date(s.create_time,'yyyy-mm-dd hh24-mi-ss'))*24*60*60 else 0 end) rest_timelength,"
				+" count(case when s.status = '4' then 1 end) rest_num"
				+" from s "
				+" group by s.user_id,  ";//分组条件统一
		
		if(null !=dateType && !"".equals(dateType)){
	    	  if("time".equals(dateType)) {
	    		  sql+="substr(s.create_time,0,13) " ;
	    		 }else if("date".equals(dateType)) { 
	    			 sql+="substr(s.create_time,0,10) " ;
	    		  }else if("month".equals(dateType)) {
	    			  sql+="substr(s.create_time,0,7) " ; 		 
	    		 }else if("year".equals(dateType)) {
	    			 sql+="substr(s.create_time,0,4) " ; 	       
	    		}		    	 
	    }else{
	    	 sql+="substr(s.create_time,0,13) " ;
	    }
		
		sql+= " ) s on t.agent_id = s.user_id and  ";
				
		if(null !=dateType && !"".equals(dateType)){
	    	  if("time".equals(dateType)) {
	    		  sql+="substr(t.create_time,0,13) " ;
	    		 }else if("date".equals(dateType)) { 
	    			 sql+="substr(t.create_time,0,10) " ;
	    		  }else if("month".equals(dateType)) {
	    			  sql+="substr(t.create_time,0,7) " ; 		 
	    		 }else if("year".equals(dateType)) {
	    			 sql+="substr(t.create_time,0,4) " ; 	       
	    		}		    	 
	    }else{
	    	 sql+="substr(t.create_time,0,13) " ;
	    }	
		
		sql+= " = s.g_time where t.reconn = '0' ";//时间条件统一
		
		if(!StringUtil.isNullStr(startDate)){
			sql+=" and t.create_time >=  '"+startDate+"'";
		}
		if(!StringUtil.isNullStr(endDate)){
			sql+=" and t.create_time <= '"+endDate+"'";
		}		
		
		if(!StringUtil.isNullStr(agentId)){
			MorphiaQuery q =  User.createQuery();
			/*q.criteria("userName").equal(p[i].fv);*/
			q.field("userName").contains(agentId);
			List<User> list = q.asList();
			agentId = "('"+agentId+"'";
			if(list != null && list.size()>0){
				for (int i = 0; i < list.size(); i++) {
					if(list.size()<=1){
						agentId = list.get(i).userId;
						sql+=" and t.agent_id = '"+agentId+"'";
					}else{
						agentId = agentId + ",'" + list.get(i).userId+"'";
					}
				}
				if(list.size()>1){
					sql+=" and t.agent_id in "+agentId+")";
				}
			}else{
				sql+=" and t.agent_id = null ";
			}
			
		}
		connect(paList, sql, p);
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(sql);
		//JdbcQueryHelper.concatQueryByTermTuples(jdbcQuery, p);
		String sbf = " group by  ";
		//截取时间分组
		String parameter = "t.agent_id";
		sbf = timeSubstr(sbf,sort,dateType,order,parameter);
       
		jdbcQuery = jdbcQuery.concat(sbf);
	    renderJSON(PageResult.Page(jdbcQuery.asList(paList.toArray()),page,rows));
	}
	
	public static void agentStat(String startDate,String endDate,String agentDn,String deptCode,QueryTermTuple[] p,String skillId, int page, int rows, String sort, String order) {
		String sql = " agent_dn,a.agent_id,agent_name, d.csat,count(time_piece) time_piece_num,sum(answer_num) answer_num,sum(notanswer_num) notanswer_num,sum(deal_timelength) deal_timelength,sum(online_timelength) online_timelength,sum(work_timelength) work_timelength, sum(effective_session_num) effective_session_num,"
	            + " round(case when sum(answer_num) > 0 and d.csat > 0 then d.csat / sum(answer_num) else 0 end,2)*100 || '%' evaluate_ratio, "
				+ " f.level_code,  f.dept_name FROM wx_stat.t_agent_stat a "
		        + " left join tbl_sys_user_rela r on a.agent_id = r.user_id"
	           	+ " left join tbl_sys_departments f on r.target_code = f.dept_code "
		        + " left join ( select a.agent_id, count(n.id) csat  from wechat_answer a left join "
		        + " wechat_satisfaction n on a.client_id = n.customer_session_id ";
		if(startDate!=null&&!"".equals(startDate)){
			sql+=" where a.create_time >= '"+ startDate.substring(0, 10) +"'";
		}
		if(endDate!=null&&!"".equals(endDate)){
			sql+=" and a.create_time < '"+endDate.substring(0, 10)+"'";
		}
		sql+= " group by a.agent_id ) d on d.agent_id = a.agent_id  "
		   + " where 1=1 ";
		if(startDate!=null&&!"".equals(startDate)){
			sql+=" and time_piece>= '"+startDate.substring(0, 10)+"'";
		}
		if(endDate!=null&&!"".equals(endDate)){
			sql+=" and time_piece< '"+endDate.substring(0, 10)+"'";
		}
		if(agentDn!=null&&!"".equals(agentDn)){
			sql+=" and agent_dn= '"+agentDn+"'";
		}
		String skillSql = "";
		if(skillId!=null&&skillId.length()>0){
			String[] skillIds = skillId.split(",");
			for(int i = 0;i<skillIds.length;i++){
				String skillIdStr = skillIds[i].trim();
				skillSql += "or skill_ids like '%"+ skillIdStr +"%'";
			}
			if(skillSql.length()>0){
				sql += " and (" + skillSql.substring(2) + ")";
			}
		}
		if(!StringUtil.isNullStr(deptCode)){
			sql += " and f.level_code like '"+deptCode+"%'";
		}
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(sql);
		JdbcQueryHelper.concatQueryByTermTuples(jdbcQuery, p);
		String sbf = " group by agent_dn,d.csat, a.agent_id, agent_name, f.level_code, f.dept_name ";
		jdbcQuery = jdbcQuery.concat(sbf);
		String sqll = jdbcQuery.getSQL();
		System.out.println("sqll-------------------------"+sqll);
		jdbcQuery.concat(" union all select '','','',0,0,sum(b.answer_num),0,0,0,0,sum(b.effective_session_num),'','',''  from (");
		jdbcQuery.concat(sqll);
		jdbcQuery.concat(")b");
		System.out.println("jdbcQuery.getSQL(-------------------------"+jdbcQuery.getSQL());
		List<Map<String, Object>> list = jdbcQuery.asList();
		renderJSON(PageResult.Page(list,page,rows));
	}
	
	public static void agentTimeIntervalStat(String startDate,String endDate,String skillId, int page, int rows) {
		//String sql = " time_piece||time_interval id,time_piece,time_interval,cons_num,agent_num,answer_timelength,notanswer_num,notanswer_time_length FROM wx_stat.t_agent_time_interval_total where 1=1 ";
		String sql = " skill_id||time_interval id,skill_id,time_interval,sum(cons_num) cons_num,sum(agent_num) agent_num,sum(answer_timelength) answer_timelength,sum(notanswer_num) notanswer_num,sum(notanswer_time_length) notanswer_time_length from wx_stat.t_agent_time_interval_total where 1=1 ";
		if(startDate!=null&&!"".equals(startDate)){
			sql+=" and time_piece>= '"+startDate.substring(0, 10)+"'";
		}
		if(endDate!=null&&!"".equals(endDate)){
			sql+=" and time_piece<= '"+endDate.substring(0, 10)+"'";
		}
		String skillSql = "";
		if(skillId!=null&&skillId.length()>0){
			String[] skillIds = skillId.split(",");
			for(int i = 0;i<skillIds.length;i++){
				String skillIdStr = skillIds[i].trim();
				skillSql += ",'"+ skillIdStr +"'";
			}
			if(skillSql.length()>0){
				sql += " and skill_id in (" + skillSql.substring(1) + ")";
			}
		}
		sql+=" group by skill_id,time_interval ";
		sql+=" order by skill_id,time_interval";
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(sql);
		List<Map<String, Object>> list = jdbcQuery.asList();
		renderJSON(PageResult.Page(list,page,rows));
	}
	
	public static void agentStatus(String startDate,String endDate,String agentName, String deptCode,int page, int rows, String sort, String order) {
		String sql = " agent_id,agent_dn,agent_name,COUNT(TIME_PIECE) time_piece_num,SUM(idle_timelength+busy_timelength+leave_timelength+rest_timelength) work_timelength,SUM(idle_timelength) idle_timelength,SUM(busy_timelength) busy_timelength,SUM(leave_timelength) leave_timelength,SUM(rest_timelength) rest_timelength,SUM(rest_num) rest_num,SUM(online_timelength) online_timelength, SUM(deal_timelength) deal_timelength,SUM(deal_num) deal_num, "
		                + " f.dept_name, f.level_code,"
				        + " case when sum(online_timelength) = 0 then '0%'  else  to_char(round((sum(idle_timelength) / sum(online_timelength))*100,2))||'%' end baifenbi FROM wx_stat.t_agent_status a "
		                + " left join tbl_sys_user_rela r on a.agent_id = r.user_id left join tbl_sys_departments f on r.target_code = f.dept_code "
		                + " where 1=1  ";
		if(startDate!=null&&!"".equals(startDate)){
			sql+=" and time_piece>= '"+startDate.substring(0, 10)+"'";
		}
		if(endDate!=null&&!"".equals(endDate)){
			sql+=" and time_piece< '"+endDate.substring(0, 10)+"'";
		}
		if(agentName!=null&&!"".equals(agentName)){
			sql+=" and agent_name like '%"+agentName+"%'";
		}
		if(!StringUtil.isNullStr(deptCode)){
			sql += " and f.level_code like '"+deptCode+"%'";
		}
		sql+=" group by agent_id,agent_dn,agent_name,f.level_code,f.dept_name";
		if(StringUtil.isNullStr(sort)){
			sql+=" order by agent_id desc";
		}
		else{
			sql+=" order by "+sort+" "+order;
		}
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(sql);
		List<Map<String, Object>> list = jdbcQuery.asList();
		renderJSON(PageResult.Page(list, page, rows));
	}//select * from wx_stat.V_AGENT_LOGIN_LOGOUT order by LOGIN_TIME desc
	public static void agentLoginLogout(String startDate,String endDate,String agentName,String agent_dn, int page, int rows, String sort, String order){
		String sql = " login_id,agent_id,agent_name,agent_dn,login_time,logout_time from wx_stat.v_agent_login_logout where 1=1 ";
		if(startDate!=null&&!"".equals(startDate)){
			sql+=" and login_time>= '"+startDate+"'";
		}
		if(endDate!=null&&!"".equals(endDate)){
			sql+=" and login_time<= '"+endDate+"'";
		}
		if(agentName!=null&&!"".equals(agentName)){
			sql+=" and agent_name like '%"+agentName+"%'";
		}
		if(agent_dn!=null&&!"".equals(agent_dn)){
			sql+=" and agent_dn like '%"+agent_dn+"%'";
		}
		if(StringUtil.isNullStr(sort)){
			sql+=" order by login_time desc";
		}
		else{
			sql+=" order by "+sort+" "+order;
		}
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(sql);
		List<Map<String, Object>> list = jdbcQuery.asList();
		renderJSON(PageResult.Page(list, page, rows));
	}
	
	public static String timeSub(String sql,String dateType){
		if(null !=dateType && !"".equals(dateType)){
	    	  if("time".equals(dateType)) {
	    		  sql+="substr(t.create_time,0,13) time_range, " ;
	    		 }else if("date".equals(dateType)) { 
	    			 sql+="substr(t.create_time,0,10) time_range, " ;
	    		  }else if("month".equals(dateType)) {
	    			  sql+="substr(t.create_time,0,7) time_range, " ; 		 
	    		 }else if("year".equals(dateType)) {
	    			 sql+="substr(t.create_time,0,4) time_range, " ; 	       
	    		}		    	 
	    }else{
	    	 sql+="substr(t.create_time,0,13) time_range, " ;
	    }
		return sql;
	}
	
	public static String timeSubstr(String sbf,String sort,String dateType,String order,String parameter){
		if(StringUtil.isNullStr(sort)){
		    if(null !=dateType && !"".equals(dateType)){
		    	  if("time".equals(dateType)) {
		    		  sbf +=" substr(t.create_time,0,13), " + parameter; 
		    		  sbf +=" order by substr(t.create_time,0,13) desc ";  
		    		 }else if("date".equals(dateType)) { 
		    			 sbf +=" substr(t.create_time,0,10), "+ parameter;  
		    			 sbf +=" order by substr(t.create_time,0,10) desc ";
		    		  }else if("month".equals(dateType)) {
		    			  sbf +=" substr(t.create_time,0,7), "+ parameter;  
		    			  sbf +=" order by substr(t.create_time,0,7) desc ";
		    		 }else if("year".equals(dateType)) {
		    			 sbf +=" substr(t.create_time,0,4), "+ parameter; 
		    			 sbf +=" order by substr(t.create_time,0,4) desc ";
		    		}		    	 
		    }else{
		    	 sbf +=" substr(t.create_time,0,13), "+ parameter;  
		    	 sbf +=" order by substr(t.create_time,0,13) desc ";
		    }
		}
		else{
			sbf = sbf + "ORDER BY "+sort+" "+order;
		}
		return sbf;
	}
	
	public static void userActive(int dateType,String fromwh,String startTime, String endTime){
		
		int endsub = 10;
		if(dateType == 3){
			endsub = 7;
		}
		String fromwhere = "";
		if(!"all".equals(fromwh)){
			fromwhere = " and channel = '"+fromwh+"'";
		}
		String where = "";
		if(dateType == 1){
			where = "where create_time>='" + startTime +"' and create_time <='"+endTime+"'"+fromwhere;
		}
		if(dateType == 2){
			where = "where substr(create_time,0,7)='"+startTime.substring(0,7)+"'"+fromwhere;
		}
		if(dateType == 3){
			where = "where substr(create_time,0,4)='"+startTime.substring(0,4)+"'"+fromwhere;
		}
		
		//String where = "where create_time>=? and create_time "+lett + "?";
		String substr = "substr(create_time,0,"+endsub+")";
		String baseSql = " count(*) count,"+substr+" day from wechat_wx_customer "+where+" group by "+substr+" order by "+substr+"";
		
		String totalSql = " count(distinct open_id) count,"+substr+" day from wechat_visitor "+where+" and user_type='user' group by "+substr+" order by "+substr+"";;
		JdbcQuery jdbcQuery = JdbcQuery.createQuery().select().concat(baseSql);
		List<Map<String, Object>> list = jdbcQuery.asList();
		Map<String,Object> map = new HashMap<String,Object>();
		
		JdbcQuery jdbcQuery2 = JdbcQuery.createQuery().select().concat(totalSql);
		List<Map<String, Object>> list2 = jdbcQuery2.asList();
		
		map.put("zlkh", list);
		map.put("zskh", list2);
		renderJSON(map);
	}
	/**
	 * 增加了一个sql与三元组拼接函数
	 * add by zengff 2018-01-18
	 * @param paList 三元组中的参数集合
	 * @param sql 拼接之前sql语句
	 * @param p 参数三元组
	 * @return 拼接之后的sql语句
	 */
	public static String connect(List paList,String sql,QueryTermTuple[] p){
		if(null!=p){
			for(QueryTermTuple q:p){
				if(null==q.fn||q.fn.equals("")||null==q.fv||q.fv.equals("")){
					continue;
				}
				boolean isFuzzy = false; 
				boolean isBeginFuzzy = false;
				boolean isEndFuzzy = false;
				boolean isNull =false;
				boolean isIn = false;
				if(sql.indexOf(" where ")!=-1){
					sql= sql + " and "+q.fn;
				}else{
					sql += " where "+q.fn;
				}
				if(null==q.lo||q.lo.equals("eq")){ //等于
					sql = sql +" = " ;
				}else if(q.lo.equals("ne")){   //不等于
					sql = sql +" != "; 
				}else if(q.lo.equals("lk")){ //开始于
					sql = sql + " like ?";
					isBeginFuzzy = true;
				}else if(q.lo.equals("nlk")){ //不开始于
					sql = sql + " not like ?";
					isBeginFuzzy = true;
				}
/*				else if(q.lo.equals("ew")){ //结束于
					sql = sql + " like ?";
					isEndFuzzy = true;
				}else if(q.lo.equals("en")){ //不结束于
					sql = sql + " not like ?";
					isEndFuzzy = true;
				}*/
				else if(q.lo.equals("cn")){  //包含
					sql = sql +" like ?";
					isFuzzy =true;
				}else if(q.lo.equals("nc")){ //不包含
					sql = sql + " not like ?";
					isFuzzy =true;
				}else if(q.lo.equals("nu")){ //为空
					sql = sql + " is null ";
					isNull =true;
				}else if(q.lo.equals("nn")){ //不为空
					sql = sql + " is not null";
					isNull =true;
				}else if(q.lo.equals("in")){ //属于
					sql = sql + " in ?";
					isIn = true;
				}else if(q.lo.equals("ni")){ //不属于
					sql = sql + " not in ?";
					isIn = true;
				}else if(q.lo.equals("lt")){ //小于
					sql = sql + " < ?";
				}else if(q.lo.equals("lte")){ //小于等于
					sql = sql + " <= ?";
				}else if(q.lo.equals("gt")){ //大于
					sql = sql + " > ?";
				}else if(q.lo.equals("gte")){ //大于等于
					sql = sql + " >= ?";
				}
				if(!isNull){
					if(isFuzzy){
						q.fv = "%"+q.fv+"%";
					}else if(isBeginFuzzy){
						q.fv = q.fv+"%";
					}else if(isEndFuzzy){
						q.fv = "%"+q.fv;
					}else if(isIn){
						q.fv = "("+q.fv+")";
					}
					paList.add(q.fv);
					
				}
			}
		}
		return sql;
	}
}
