package com.youku.wireless.guid;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.WritableComparable;

public class guid_distinct_merge_request implements WritableComparable<guid_distinct_merge_request> {

	// args
	private String pid;
	private String ver;
	private String operator;
	private String network;
	private String brand;
	private String btype;
	private String os;
	private String os_ver;
	private String wt;
	private String ht;
	private String imei;
	private String imsi;
	private String mobile;
	private String mac;
	private String uuid;
	//private String time;
	private String guid;
	private String guid2;
	private String deviceid;
	private String ndeviceid;
	private long longtime;

	public guid_distinct_merge_request() {
	}

	public guid_distinct_merge_request(String line) {
		try {
			String[] tmp = line.split("\t");
			
			guid = tmp[0];
			guid2 = tmp[1];
			pid = tmp[2];
			os = tmp[3];
			os_ver = tmp[4];
			btype = tmp[5];
			brand = tmp[6];
			operator = tmp[7];
			deviceid = tmp[8];
			ndeviceid = tmp[9];
			longtime = Long.valueOf(tmp[10]).longValue();
			ver = tmp[11];
			mac = tmp[12];
			imei = tmp[13];
			uuid = tmp[14];
//			
//				if (pid == null) {
//					pid = "null";
//				}
//				if (ver == null) {
//					ver = "N/A";
//				}
//				ver = ver.replaceAll("[^0-9.]", "");
//				ver = ver.equals("") ? "N/A" : ver;
//				
//				if (imei == null) {
//					imei = "null";
//				}else{
//					imei = imei.replaceAll("[\r\n\"]", "");
//					imei = imei.equals("") ? "null" : imei;
//				}
//				if (imsi == null) {
//					imsi = "null";
//				}else{
//					imsi = imsi.replaceAll("[\r\n\"]", "");
//					imsi = imsi.equals("") ? "null" : imsi;
//				}
//				if (deviceid == null) {
//					deviceid = "null";
//				}else{
//					deviceid = deviceid.replaceAll("[\r\n\"]", "");
//					deviceid = deviceid.equals("") ? "null" : deviceid;
//				}
//				if (ndeviceid == null) {
//					ndeviceid = "null";
//				}else{
//					ndeviceid = ndeviceid.replaceAll("[\r\n\"]", "");
//					ndeviceid = ndeviceid.equals("") ? "null" : ndeviceid;
//				}
//				if (mac == null) {
//					mac = "null";
//				}else{
//					mac = mac.replaceAll("[\r\n\"]", "");
//					mac = mac.equals("") ? "null" : mac;
//				}
//				if (uuid == null) {
//					uuid = "null";
//				}else{
//					uuid = uuid.replaceAll("[\r\n\"]", "");
//					uuid = uuid.equals("") ? "null" : uuid;
//				}
//				if (operator == null) {
//					operator = "null";
//				} else {
//					operator = URLDecoder.decode(operator, "UTF-8");
//					operator = operator.replaceAll("[\r\n\"]", "");
//					operator = operator.equals("") ? "null" : operator;
//				}
//				if (network == null) {
//					network = "null";
//				}else{
//					network = network.replaceAll("[\r\n\"]", "");
//					network = network.equals("") ? "null" : network;
//				}
//				if (brand == null) {
//					brand = "null";
//				}else{
//					brand = brand.replaceAll("[\r\n\"]", "");
//					brand = brand.equals("") ? "null" : brand;
//				}
//				if (btype == null) {
//					btype = "null";
//				} else {
//					btype = URLDecoder.decode(btype, "UTF-8");
//					btype = btype.replaceAll("[\r\n\"]", "");
//					btype = btype.equals("") ? "null" : btype;
//				}
//				if (os == null) {
//					os = "null";
//				}else{
//					os = os.replaceAll("[\r\n\"]", "");
//					os = os.equals("") ? "null" : os;
//				}
//				if (os_ver == null) {
//					os_ver = "N/A";
//				}else{
//					os_ver = os_ver.replaceAll("[\r\n\"]", "");
//				}
//				os_ver = os_ver.replaceAll("[^0-9.]", "");
//				os_ver = os_ver.equals("") ? "N/A" : os_ver;
//				if (wt == null) {
//					wt = "null";
//				}else{
//					wt = wt.replaceAll("[\r\n\"]", "");
//					wt = wt.equals("") ? "null" : wt;
//				}
//				if (ht == null) {
//					ht = "null";
//				}else{
//					ht = ht.replaceAll("[\r\n\"]", "");
//					ht = ht.equals("") ? "null" : ht;
//				}
//				if(guid == null){
//					guid = "null";
//				}
//				if(guid2 == null){
//					guid2 = "null";
//				}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public String toString() {
		return "Paike [ver=" + ver + "]";
	}

	public String getPid() {
		return pid;
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

		// args
		out.writeUTF(pid);
		out.writeUTF(ver);
		out.writeUTF(operator);
//		out.writeUTF(network);
		out.writeUTF(brand);
		out.writeUTF(btype);
		out.writeUTF(os);
		out.writeUTF(os_ver);
//		out.writeUTF(wt);
//		out.writeUTF(ht);
		out.writeUTF(imei);
//		out.writeUTF(imsi);
		out.writeUTF(guid2);
		out.writeUTF(mac);
		out.writeUTF(uuid);
		//out.writeUTF(time);
		out.writeUTF(guid);
		out.writeUTF(deviceid);
		out.writeUTF(ndeviceid);

		// longtime
		out.writeLong(longtime);

	}

	@Override
	public void readFields(DataInput in) throws IOException {

		// args
		pid = in.readUTF();
		ver = in.readUTF();
		operator = in.readUTF();
//		network = in.readUTF();
		brand = in.readUTF();
		btype = in.readUTF();
		os = in.readUTF();
		os_ver = in.readUTF();
//		wt = in.readUTF();
//		ht = in.readUTF();
		imei = in.readUTF();
//		imsi = in.readUTF();
		guid2 = in.readUTF();
		mac = in.readUTF();
		uuid = in.readUTF();
		//time = in.readUTF();
		guid = in.readUTF();
		deviceid = in.readUTF();
		ndeviceid = in.readUTF();
		longtime = in.readLong();
	}

	@Override
	public int compareTo(guid_distinct_merge_request r) {
		if (r == null) {
			return 0;
		}
		return 0;
	}

	public String getOperator() {
		return operator;
	}

	public void setOperator(String operator) {
		this.operator = operator;
	}

	public String getNetwork() {
		return network;
	}

	public void setNetwork(String network) {
		this.network = network;
	}

	public String getBrand() {
		return brand;
	}

	public void setBrand(String brand) {
		this.brand = brand;
	}

	public String getBtype() {
		return btype;
	}

	public void setBtype(String btype) {
		this.btype = btype;
	}

	public String getOs() {
		return os;
	}

	public void setOs(String os) {
		this.os = os;
	}

	public String getOs_ver() {
		return os_ver;
	}

	public void setOs_ver(String os_ver) {
		this.os_ver = os_ver;
	}

	public String getWt() {
		return wt;
	}

	public void setWt(String wt) {
		this.wt = wt;
	}

	public String getHt() {
		return ht;
	}

	public void setHt(String ht) {
		this.ht = ht;
	}

	public String getImei() {
		return imei;
	}

	public void setImei(String imei) {
		this.imei = imei;
	}

	public String getImsi() {
		return imsi;
	}

	public void setImsi(String imsi) {
		this.imsi = imsi;
	}

	public String getMobile() {
		return mobile;
	}

	public void setMobile(String mobile) {
		this.mobile = mobile;
	}

	public String getMac() {
		return mac;
	}

	public void setMac(String mac) {
		this.mac = mac;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public String getDeviceid() {
		return deviceid;
	}

	public void setDeviceid(String deviceid) {
		this.deviceid = deviceid;
	}

	public String getNdeviceid() {
		return ndeviceid;
	}

	public void setNdeviceid(String ndeviceid) {
		this.ndeviceid = ndeviceid;
	}

	public long getLongtime() {
		return longtime;
	}

	public void setLongtime(long longtime) {
		this.longtime = longtime;
	}

	public void setPid(String pid) {
		this.pid = pid;
	}

	public String getGuid2() {
		return guid2;
	}

	public void setGuid2(String guid2) {
		this.guid2 = guid2;
	}

}