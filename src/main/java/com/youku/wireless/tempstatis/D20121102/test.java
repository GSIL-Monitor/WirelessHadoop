package com.youku.wireless.tempstatis.D20121102;

public class test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
//		String line = "117.136.17.133 - - [01/Nov/2012:16:33:59 +0800] \"GET /openapi-common/getVideoDetail?pid=062de1d9c3d4a310&vid=71450605&f=7 HTTP/1.1\" 200 873 \"-\" \"Dalvik/1.4.0%20(Linux;%20U;%20Android%202.3.4;%20GT-S5570%20Build/GINGERBREAD)\"";
//		String line2 = "27.156.165.2 ios.youku.com - [31/Oct/2012:00:22:05 +0800] \"HEAD /iphone/videos/115875815/play?system=4.2.1&machine=iPhone3,1&uuid=05826fa7a3de97cf7bd5742109c501dab64cfd3a HTTP/1.1\" 302 0 \"-\" \"Youku/1.3.1%20CFNetwork/485.12.7%20Darwin/10.4.0\"";
//		String line3 = "221.11.29.158 \"2012-10-31T00:01:43+08:00\" GET \"/openapi-wireless/getVideoDetail\" \"pid=00c448a8e7c55f09&vid=XNDU4MDY0MzUy&uid=b1b68b1cadf80e6cdae890922e999bca&format=4&rt=2\" \"-\" 200 1763 0.001 \"Dalvik/1.4.0%20(Linux;%20U;%20Android%202.3.6;%20YP-G70%20Build/GINGERBREAD)\"";
//		String line4 = "116.92.25.130 \"2012-10-31T16:58:23+08:00\" POST \"/v1/videos/XNDY3Njc2NDA0/playing\" \"-\" \"sessionid=6D04DF562A7ECF55&uid=null&network=wifi&isp=&event_type=begin&play_type=online&play_codes=0&mode=0&pid=40094199e8116300&ver=1.5&operator=_45400&guid=df1060b3589a3e9c435366c8e66297bc\" 200 0 0.298 \"Paike;1.5;Android;2.3.6;GT-S7500\"";
//		vv_Channel_statis_request v = new vv_Channel_statis_request();
//		v.vv_Channel_statis_request_go(line);
//		System.out.println(v.toString()+"---"+v.getDate()+"---"+v.getDate_day());
//		
//		
//		v = new vv_Channel_statis_request();
//		v.vv_Channel_statis_request_ios(line2);
//		System.out.println(v.toString()+"---"+v.getDate()+"---"+v.getDate_day());
//		
//		v = new vv_Channel_statis_request();
//		v.vv_Channel_statis_request_api2(line3);
//		System.out.println(v.toString()+"---"+v.getDate()+"---"+v.getDate_day());
//		
//		v = new vv_Channel_statis_request();
//		v.vv_Channel_statis_request_paike(line4);
//		System.out.println(v.toString()+"---"+v.getDate()+"---"+v.getDate_day());
//		
//		
//		String line5 = "100008363	03000202004F80E3B6858701362F2DC7552E07-2739-1F77-82BE-7E2CD4CDF5E6	中国算命术 06	73237369	zrk探索者	中国算命术	598.12	1477604	Mon May 21 21:16:38 CST 2012	null	null	null	92	0	0.00    0       0       0       0       34398552        1       0       Mon May 21 15:40:00 CST 2012    9de3fbf0dff9d9aa18c0c021598b5627        1       0UPLOAD";
//		String[] catinfo = line5.split("\t");
//		if(catinfo.length > 12){
//			System.out.println("line0:"+catinfo[0]);
//			System.out.println("line13:"+catinfo[12]);
//		}
		
		//System.out.println("0238921.183912".matches("[0-9]+"));
		System.out.println(("aaaaaaa0".hashCode() & 1100) % 10);
		System.out.println(("aaaaaaa1".hashCode() & 1100) % 10);
		System.out.println("bbbbbbbbbv0".hashCode() & Integer.MAX_VALUE);
		System.out.println("bbbbbbbbbv1".hashCode() & Integer.MAX_VALUE);
	}

}
