package com.youku.wireless.tempstatis.D20121102;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair>{
	
	private String prov;
	private String city;
	private String vid;
	private String count;
	private String date;
	
	private String cid;
	
	private int flag;
	
	private String strflag;
	
	public TextPair(){
		
	}
	
	public TextPair(String vid, String strflag){
		if(vid == null){
			vid = "";
		}
		if(strflag == null){
			strflag = "";
		}
		this.vid = vid;
		this.strflag = strflag;
		
		if(prov == null){
			prov = "";
		}
		if(city == null){
			city = "";
		}
		if(cid == null){
			cid = "";
		}
		if(count == null){
			count = "";
		}
		if(date == null){
			date = "";
		}
	}
	
	@Override
	public int compareTo(TextPair o) {
		int cmp = vid.compareTo(o.vid);
		if(cmp != 0){
			return cmp;
		}
		
		return strflag.compareTo(o.strflag);
	}
	
	@Override
	public int hashCode(){
		return vid.hashCode() + strflag.hashCode();
	}
	
	@Override
	public boolean equals(Object o){
		if(o instanceof TextPair){
			TextPair tp = (TextPair)o;
			return vid.equals(tp.vid) && strflag.equals(tp.strflag);
		}
		return false;
	}
	
	@Override
	public String toString(){
		return vid + "-" + strflag;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		prov = in.readUTF();
		date = in.readUTF();
		city = in.readUTF();
		vid = in.readUTF();
		count = in.readUTF();
		cid = in.readUTF();
		strflag = in.readUTF();
		flag = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(prov);
		out.writeUTF(date);
		out.writeUTF(city);
		out.writeUTF(vid);
		out.writeUTF(count);
		out.writeUTF(cid);
		out.writeUTF(strflag);
		out.writeInt(flag);
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

	public String getStrflag() {
		return strflag;
	}

	public void setStrflag(String strflag) {
		this.strflag = strflag;
	}
	
}
