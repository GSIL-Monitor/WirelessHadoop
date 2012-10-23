package com.youku.wireless.statis;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.WritableComparable;

public class vv_statis_request implements WritableComparable<vv_statis_request> {
	// pattern
	//private static final String realLogEntryPattern = "^([\\d.]+) \"(\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2})\" (\\S+) \"(\\S+)\" \"(.+)\" \"(\\S*)\" ([\\d]+) ([\\d]+) ([\\d]+\\.[\\d]+) \"(.*)\" ([\\d]+) ([\\d]+)";
	
	private static final String realLogEntryPattern = "^([\\d.]+) \"(\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2})\\+\\d{2}:\\d{2}\" (\\S+) \"(\\S+)\" \"(.+)\" \"(\\S*)\" ([\\d]+) ([\\d]+) ([\\d]+\\.[\\d]+) \"(.*)\"";
	// http
	private String ip;
	private String date;
	private String method;
	private String uri;
	private String response_code;
	private String content_length;
	private String request_time;
	private String user_agent;
	private String request_args;
	private String request_body;


	// args
	private String pid;
	private String play_codes;
	private String play_type;
	private String sessionid;
	private String type;
	private String complete;
	private String ver;
	private String guid;
	private String date_day;

	// labels
	private static final String pid_label = "pid";
	private static final String play_codes_label = "play_codes";
	private static final String play_type_label = "play_types";
	private static final String sessionid_label = "sessionid";
	private static final String type_label = "type";
	private static final String complete_label = "complete";
	private static final String ver_label = "ver";
	private static final String guid_label = "guid";

	public vv_statis_request() {
	}

	public vv_statis_request(String line) {
		try {
			Pattern realP = Pattern.compile(realLogEntryPattern);
			Matcher realMatcher = realP.matcher(line);
			if (realMatcher.matches()) {
				ip = realMatcher.group(1);
				date = realMatcher.group(2);
				method = realMatcher.group(3);
				uri = realMatcher.group(4);
				request_args = realMatcher.group(5);
				request_body = realMatcher.group(6);
				response_code = realMatcher.group(7);
				content_length = realMatcher.group(8);
				request_time = realMatcher.group(9);
				
				request_args = request_args+"&"+request_body;
				String[] args = request_args.split("&");
				Map<String, String> map = new HashMap<String, String>();
				for (String arg : args) {
					if (arg==null || arg.equals("=")){
						continue;
					}
					String[] key_value = arg.split("=");
					String key = key_value[0];
					String value = key_value.length == 2 ? key_value[1] : "";
					map.put(key, value);
				}
				pid = (String) map.get(pid_label);
				if (pid == null) {
					pid = "";
				}
				
				ver = (String)map.get(ver_label);
				if (ver == null || ver.equals("")){
					ver = "N/A";
				}else{
					ver = ver.replaceAll("[^0-9.]", "");
					if (ver == null || ver.equals("")){
						ver = "N/A";
					}
				}
				
				guid = (String)map.get(guid_label);
				if (guid == null){
					guid = "";
				}
				
				if(date!=null && !date.equals("")){
					date_day = timeCompare(date);
				}
				
			} else {
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return "Paike [ver=" + ver + "]";
	}


	public String getRequest_args() {
		return request_args;
	}

	public String getIp() {
		return ip;
	}

	public String getDate() {
		return date;
	}

	public String getMethod() {
		return method;
	}

	public String getUri() {
		return uri;
	}

	public String getResponse_code() {
		return response_code;
	}

	public String getContent_length() {
		return content_length;
	}

	public String getRequest_time() {
		return request_time;
	}

	public String getUser_agent() {
		return user_agent;
	}

	public String getPid() {
		return pid;
	}

	public String getPlay_codes() {
		return play_codes;
	}

	public void setPlay_codes(String play_codes) {
		this.play_codes = play_codes;
	}

	public String getPlay_type() {
		return play_type;
	}

	public void setPlay_type(String play_type) {
		this.play_type = play_type;
	}

	public String getSessionid() {
		return sessionid;
	}

	public void setSessionid(String sessionid) {
		this.sessionid = sessionid;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getDate_day() {
		return date_day;
	}

	public void setDate_day(String date_day) {
		this.date_day = date_day;
	}

	public String getComplete() {
		return complete;
	}

	public void setComplete(String complete) {
		this.complete = complete;
	}

	public String getVer() {
		return ver;
	}

	public void setVer(String ver) {
		this.ver = ver;
	}

	public String getGuid() {
		return guid;
	}

	public void setGuid(String guid) {
		this.guid = guid;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(ip);
		out.writeUTF(date);
		out.writeUTF(method);
		out.writeUTF(uri);
		out.writeUTF(response_code);
		out.writeUTF(content_length);
		out.writeUTF(request_time);
		//out.writeUTF(user_agent);
		out.writeUTF(request_args);

		out.writeUTF(pid);
		//out.writeUTF(play_type);
		//out.writeUTF(play_codes);
		
		//out.writeUTF(sessionid);
		//out.writeUTF(type);
		out.writeUTF(ver);
		//out.writeUTF(complete);
		
		out.writeUTF(guid);
		
		out.writeUTF(date_day);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		ip = in.readUTF();
		date = in.readUTF();
		method = in.readUTF();
		uri = in.readUTF();
		response_code = in.readUTF();
		content_length = in.readUTF();
		request_time = in.readUTF();
		//user_agent = in.readUTF();
		request_args = in.readUTF();

		pid = in.readUTF();
		//play_type = in.readUTF();
		//play_codes = in.readUTF();
		
		//sessionid = in.readUTF();
		//type = in.readUTF();
		//complete = in.readUTF();
		ver = in.readUTF();
		
		guid = in.readUTF();
		
		date_day = in.readUTF();
	}

	@Override
	public int compareTo(vv_statis_request r) {
		if (r == null) {
			return 0;
		}
		return 0;
	}
	
	public String timeCompare(String time1){
		
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
			Date d = dateFormat.parse(time1);
			SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyyMMdd");
			return dateFormat1.format(d);
			
		} catch (ParseException e) {
			return "";
		}
	}

}