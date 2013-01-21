package com.youku.wireless.utils;

import com.youku.wireless.guid.guid_statis_request;

public class LogStrUtils {
	
	public static String getLineFromHourfile(String hourline){
		try{
			int start = 0;
			start=hourline.substring(0, hourline.indexOf(".")).lastIndexOf(" ");
			start++;
			return hourline.substring(start);
		}catch(Exception e){
			e.printStackTrace();
		}
		return "";
	}

	public static void main(String[] args) {
		String a = "Dec 12 18:43:39 a15 120.72.49.222 \"2012-12-12T18:43:39+08:00\" POST \"/openapi-wireless/initial\" \"pid=dbdc7787bb298bc3&ver=2.4.3&network=WIFI&brand=samsung&btype=GT-I9100G&os=Android&os_ver=2.3.6&wt=480&ht=800&imei=352110058771818&imsi=&mobile=&mac=20:64:32:4F:5B:EB&uuid=&time=\" \"-\" 200 114 0.006 \"Youku;2.4.3;Android;2.3.6;GT-I9100G\"";
		//a = "106.120.71.193 \"2012-12-12T15:16:42+08:00\" POST \"/openapi-wireless/initial\" \"pid=dbdc7787bb298bc3&ver=2.4.3&operator=CMCC_46002&network=WIFI&brand=google&btype=Nexus+S&os=Android&os_ver=4.1.2&wt=480&ht=800&imei=355921041723345&imsi=89860011010708661571&mobile=&mac=78:d6:f0:26:4c:d6&uuid=&time=\" \"-\" 200 114 0.004 \"Youku;2.4.3;Android;4.1.2;Nexus%20S\"";
		//a = "a15 120.72.49.222 \"2012-12-12T18:43:39+08:00\" POST \"/openapi-wireless/initial\" \"pid=dbdc7787bb298bc3&ver=2.4.3&network=WIFI&brand=samsung&btype=GT-I9100G&os=Android&os_ver=2.3.6&wt=480&ht=800&imei=352110058771818&imsi=&mobile=&mac=20:64:32:4F:5B:EB&uuid=&time=\" \"-\" 200 114 0.006 \"Youku;2.4.3;Android;2.3.6;GT-I9100G\"";
		guid_statis_request g = new guid_statis_request(getLineFromHourfile(a));

		System.out.println(getLineFromHourfile(a) + " -- " + g.getGuid() + "-" + g.getPid());
	}
}
