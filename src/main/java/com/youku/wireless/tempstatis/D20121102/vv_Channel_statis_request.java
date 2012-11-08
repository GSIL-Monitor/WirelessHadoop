package com.youku.wireless.tempstatis.D20121102;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.WritableComparable;

import com.youku.wireless.utils.VideoH64Base;

public class vv_Channel_statis_request implements WritableComparable<vv_Channel_statis_request> {
	// pattern
	private static final String realLogEntryPattern = "^([\\d.]+) \"(\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2})\\+\\d{2}:\\d{2}\" (\\S+) \"(\\S+)\" \"(.+)\" \"(\\S*)\" ([\\d]+) ([\\d]+) ([\\d]+\\.[\\d]+) \"(.*)\"";
	private static final String goLogEntryPattern = "^([\\d.]+) - - \\[(\\d{2}\\/[a-zA-Z]{3}\\/\\d{4}:\\d{2}:\\d{2}:\\d{2}) \\+\\d{4}\\] \".*? (\\S+) \\S+\" [\\d]+ [\\d]+ \".*?\" \".*?\"";
	private static final String iosLogEntryPattern = "^([\\d.]+) \\S+ - \\[(\\d{2}\\/[a-zA-Z]{3}\\/\\d{4}:\\d{2}:\\d{2}:\\d{2}) \\+\\d{4}\\] \".*? (\\S+) \\S+\" [\\d]+ [\\d]+ \".*?\" \".*?\"";
	private static final String api2LogEntryPattern = "^([\\d.]+) \"(\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2})\\+\\d{2}:\\d{2}\" (\\S+) \"(\\S+)\" \"(.+)\" \"(\\S*)\" ([\\d]+) ([\\d]+) ([\\d]+\\.[\\d]+) \"(.*)\"";
	private static final String paikeLogEntryPattern = "^([\\d.]+) \"(\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2})\\+\\d{2}:\\d{2}\" (\\S+) \"(\\S+)\" \"(.+)\" \"(\\S*)\" ([\\d]+) ([\\d]+) ([\\d]+\\.[\\d]+) \"(.*)\"";
	
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
	private String date_day;
	private String vid;

	// labels
	private static final String pid_label = "pid";
	private static final String vid_label = "vid";

	public vv_Channel_statis_request() {
	}

	public void vv_Channel_statis_request_api3(String line) {
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
				
				if(method == null || (!method.equals("GET") && !method.equals("POST"))){
					return;
				}
				
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
				
				vid = (String) map.get(vid_label);
				if(vid == null){
					vid = (String)map.get("id");
				}
				
				if(vid != null){
					vid = vid.replaceAll("%3D", "=");
					vid = vid.replaceAll("%20", "");
					if(!vid.matches("[0-9]+")){
						vid = VideoH64Base.getFromBASE64(vid);
					}
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
	
	public void vv_Channel_statis_request_api2(String line) {
		try {
			Pattern realP = Pattern.compile(api2LogEntryPattern);
			Matcher realMatcher = realP.matcher(line);
			if (realMatcher.matches()) {
				ip = realMatcher.group(1);
				date = realMatcher.group(2);
				method = realMatcher.group(3);
				uri = realMatcher.group(4);
				request_args = realMatcher.group(5);
				request_body = realMatcher.group(6);
				//response_code = realMatcher.group(7);
				//content_length = realMatcher.group(8);
				//request_time = realMatcher.group(9);
				
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
				
				vid = (String) map.get(vid_label);
				if(vid != null){
					vid = vid.replaceAll("%3D", "=");
					vid = vid.replaceAll("%20", "");
					if(!vid.matches("[0-9]+")){
						vid = VideoH64Base.getFromBASE64(vid);
					}
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
	
	public void vv_Channel_statis_request_paike(String line) {
		try {
			Pattern realP = Pattern.compile(paikeLogEntryPattern);
			Matcher realMatcher = realP.matcher(line);
			if (realMatcher.matches()) {
				ip = realMatcher.group(1);
				date = realMatcher.group(2);
				method = realMatcher.group(3);
				uri = realMatcher.group(4);
				request_args = realMatcher.group(5);
				request_body = realMatcher.group(6);
				//response_code = realMatcher.group(7);
				//content_length = realMatcher.group(8);
				//request_time = realMatcher.group(9);
				
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
				
				if(uri!=null && uri.contains("/")){
					request_args = uri.substring(uri.indexOf("?")+1);
					if (request_args!=null && !request_args.equals("")){
						String[] urls = uri.split("/");
						vid = urls[3];
					}
					
				}
				if(vid != null){
					vid = vid.replaceAll("%3D", "=");
					vid = vid.replaceAll("%20", "");
					if(!vid.matches("[0-9]+")){
						vid = VideoH64Base.getFromBASE64(vid);
					}
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
	
	public void vv_Channel_statis_request_go(String line) {
		try {
			Pattern realP = Pattern.compile(goLogEntryPattern);
			Matcher realMatcher = realP.matcher(line);
			if (realMatcher.matches()) {
				ip = realMatcher.group(1);
				date = realMatcher.group(2);
				//method = realMatcher.group(3);
				uri = realMatcher.group(3);
				request_args = "";
				request_body = "";
				response_code = "";
				content_length = "";
				request_time = "";
				
				request_args = "";
				if(uri!=null && uri.contains("?")){
					request_args = uri.substring(uri.indexOf("?")+1);
					if (request_args!=null && !request_args.equals("")){
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
						vid = (String)map.get(vid_label);
					}
				}
				
				if(pid == null) {
					pid = "";
				}
				
				if(vid != null){
					vid = vid.replaceAll("%3D", "=");
					vid = vid.replaceAll("%20", "");
					if(!vid.matches("[0-9]+")){
						vid = VideoH64Base.getFromBASE64(vid);
					}
				}
				
				if(date!=null && !date.equals("")){
					date_day = timeCompare1(date);
				}
				
			} else {
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void vv_Channel_statis_request_ios(String line) {
		try {
			Pattern realP = Pattern.compile(iosLogEntryPattern);
			Matcher realMatcher = realP.matcher(line);
			if (realMatcher.matches()) {
				ip = realMatcher.group(1);
				date = realMatcher.group(2);
				//method = realMatcher.group(3);
				uri = realMatcher.group(3);
				request_args = "";
				request_body = "";
				response_code = "";
				content_length = "";
				request_time = "";
				
				request_args = "";
				if(uri!=null && uri.contains("/")){
					request_args = uri.substring(uri.indexOf("?")+1);
					if (request_args!=null && !request_args.equals("")){
						String[] args = uri.split("/");
						vid = args[3];
						pid = "5e6b1ec70d0bee0c";
					}
					
				}
				
				if(vid!=null){
					vid = vid.replaceAll("%3D", "=");
					vid = vid.replaceAll("%20", "");
					if(!vid.matches("[0-9]+")){
						vid = VideoH64Base.getFromBASE64(vid);
					}
				}
				
				if(pid==null){
					pid = "";
				}
				
				if(date!=null && !date.equals("")){
					date_day = timeCompare1(date);
				}
				
			} else {
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return "Channel vv [vid=" + vid + ", pid=" + pid + "]";
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


	public String getDate_day() {
		return date_day;
	}

	public void setDate_day(String date_day) {
		this.date_day = date_day;
	}

	public String getVid() {
		return vid;
	}

	public void setVid(String vid) {
		this.vid = vid;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(ip);
		out.writeUTF(date);
		out.writeUTF(pid);
		out.writeUTF(vid);
		
		out.writeUTF(date_day);

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		ip = in.readUTF();
		date = in.readUTF();
		pid = in.readUTF();
		vid = in.readUTF();
		
		date_day = in.readUTF();
	}

	@Override
	public int compareTo(vv_Channel_statis_request r) {
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
	
	public String timeCompare1(String time1){
		
		try {
			SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",Locale.ENGLISH);
			Date d = dateFormat.parse(time1);
			SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyyMMdd");
			return dateFormat1.format(d);
			
		} catch (ParseException e) {
			return "";
		}
	}

}