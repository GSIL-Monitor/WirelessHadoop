package com.youku.wireless.tempstatis.D20121102;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


import com.youku.wireless.utils.VideoH64Base;

public class Video_Cat_Statis_request {
	//private static final String realLogEntryPattern = "(\\d+)-(\\S+)-(\\S+)-(\\S+)\t(\\d+)";
	
	private String prov;
	private String city;
	private String vid;
	private String count;
	private String date;
	
	private String cid;
	
	private int flag=0; //2:频道统计结果  1:vid名称


	public Video_Cat_Statis_request() {
	}

	public Video_Cat_Statis_request(String line) {
		try {
			String[] catinfo = line.split("\t");

			if (catinfo.length > 13) {
				flag = 1;
				vid = catinfo[0];
				cid = catinfo[12];
			}else{
				flag = 2;
//				Pattern realP = Pattern.compile(realLogEntryPattern);
//				Matcher realMatcher = realP.matcher(line);
//				if (realMatcher.matches()) {
//					date = realMatcher.group(1);
//					prov = realMatcher.group(2);
//					city = realMatcher.group(3);
//					vid = realMatcher.group(4);
//					count = realMatcher.group(5);
//					
//					vid = vid.replaceAll("%3D", "=");
//					vid = vid.replaceAll("%20", "");
//					if(!vid.matches("[0-9]+")){
//						vid = VideoH64Base.getFromBASE64(vid);
//					}
//				}
				String[] tmp = line.split("\t");
				if(tmp.length == 2){
					count = tmp[1];
					String[] tmp1 = tmp[0].split("-");
					if(tmp1.length == 4){
						date = tmp1[0];
						prov = tmp1[1];
						city = tmp1[2];
						vid = tmp1[3];
						
						vid = vid.replaceAll("%3D", "=");
						vid = vid.replaceAll("%20", "");
						if(!vid.matches("[0-9]+")){
							vid = VideoH64Base.getFromBASE64(vid);
						}
					}else{
						return;
					}
				}else{
					return;
				}
			}
			
			if(date == null){
				date = "";
			}
			
			if(prov == null){
				prov = ""; 
			}
			
			if(city == null){
				city = "";
			}
			
			if(count == null){
				count = "";
			}
			
			if(cid == null){
				cid = "";
			}
			
			if(vid == null){
				vid = "";
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
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
			SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
			Date d = dateFormat.parse(time1);
			SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyyMMdd");
			return dateFormat1.format(d);
			
		} catch (ParseException e) {
			return "";
		}
	}

	public String getProv() {
		return prov;
	}

	public void setProv(String prov) {
		this.prov = prov;
	}

	public String getCity() {
		return city;
	}

	public void setCity(String city) {
		this.city = city;
	}

	public String getVid() {
		return vid;
	}

	public void setVid(String vid) {
		this.vid = vid;
	}

	public String getCount() {
		return count;
	}

	public void setCount(String count) {
		this.count = count;
	}

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public String getCid() {
		return cid;
	}

	public void setCid(String cid) {
		this.cid = cid;
	}

	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}

}