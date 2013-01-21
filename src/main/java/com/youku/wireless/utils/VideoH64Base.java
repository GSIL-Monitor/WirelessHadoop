package com.youku.wireless.utils;

import sun.misc.BASE64Decoder;

public class VideoH64Base {
	// 将 s 进行 BASE64 编码 
	public static String getBASE64(String s) { 
		long vid = Long.valueOf(s);
		vid = vid << 2;
		String vidstr = Long.toString(vid);
		return (new StringBuffer("X").append(new sun.misc.BASE64Encoder().encode( vidstr.getBytes()))).toString(); 
	} 

	// 将 BASE64 编码的字符串 s 进行解码 
	public static String getFromBASE64(String s) { 
	if (s == null) return null; 
	String vid = s.substring(1);
	BASE64Decoder decoder = new BASE64Decoder(); 
	try { 
	byte[] b = decoder.decodeBuffer(vid); 
	vid = new String(b);
	
	long vid0 = Long.valueOf(vid).longValue() >> 2;
		return String.valueOf(vid0);
	} catch (Exception e) { 
	return null; 
	} 
	}
	
	public static void main(String[] args){
		String vid = "86191888";
		String vid0 = getBASE64(vid);
		String vid1 = getFromBASE64("XMzQ0NzY3NTUy");
		System.out.println("getBASE64:"+vid0);
		//vid0="";
		if(vid0.matches("[a-zA-Z=]+")){
			System.out.println("xxxxx"+vid0);
		}
		System.out.println("getFromBASE64:"+vid1);
	}
}
