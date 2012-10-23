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

public class guid_statis_request implements WritableComparable<guid_statis_request> {
	private static final String realLogEntryPattern = "^([\\d.]+) \"(\\d{4}\\-\\d{2}\\-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+\\d{2}:\\d{2})\" (\\S+) \"(\\S+)\" \"(.+)\" \"(\\S*)\" ([\\d]+) ([\\d]+) ([\\d]+\\.[\\d]+) \"(.*)\"";
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

	// labels
	private static final String pid_label = "pid";
	private static final String ver1_label = "ver";
	private static final String ver2_label = "version";
	private static final String operator_label = "operator";
	private static final String network_label = "network";
	private static final String brand_label = "brand";
	private static final String btype_label = "btype";
	private static final String os_label = "os";
	private static final String os_ver_label = "os_ver";
	private static final String wt_label = "wt";
	private static final String ht_label = "ht";
	private static final String imei_label = "imei";
	private static final String imsi_label = "imsi";
	//private static final String mobile_label = "mobile";
	private static final String mac_label = "mac";
	private static final String uuid_label = "uuid";
	//private static final String time_label = "time";
	private static final String deviceid_label = "deviceid";
	private static final String ndeviceid_label = "ndeviceid";
	
	private static MessageDigest MD5_DIGEST = null;

	static {
		try {
			MD5_DIGEST = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("MD5 not supported", e);
		}
	}

	public guid_statis_request() {
	}

	public guid_statis_request(String line) {
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
					pid = "null";
				}
				ver = (String) map.get(ver1_label);
				if (ver == null) {
					ver = (String) map.get(ver2_label);
					if (ver == null) {
						ver = "N/A";
					}
				}
				ver = ver.replaceAll("[^0-9.]", "");
				ver = ver.equals("") ? "N/A" : ver;
				
				imei = (String) map.get(imei_label);
				if (imei == null) {
					imei = "null";
				}else{
					imei = imei.replaceAll("[\r\n\"]", "");
					imei = imei.equals("") ? "null" : imei;
				}
				imsi = (String) map.get(imsi_label);
				if (imsi == null) {
					imsi = "null";
				}else{
					imsi = imsi.replaceAll("[\r\n\"]", "");
					imsi = imsi.equals("") ? "null" : imsi;
				}
				deviceid = (String) map.get(deviceid_label);
				if (deviceid == null) {
					deviceid = "null";
				}else{
					deviceid = deviceid.replaceAll("[\r\n\"]", "");
					deviceid = deviceid.equals("") ? "null" : deviceid;
				}
				ndeviceid = (String) map.get(ndeviceid_label);
				if (ndeviceid == null) {
					ndeviceid = "null";
				}else{
					ndeviceid = ndeviceid.replaceAll("[\r\n\"]", "");
					ndeviceid = ndeviceid.equals("") ? "null" : ndeviceid;
				}
				mac = (String) map.get(mac_label);
				if (mac == null) {
					mac = "null";
				}else{
					mac = mac.replaceAll("[\r\n\"]", "");
					mac = mac.equals("") ? "null" : mac;
				}
				uuid = (String) map.get(uuid_label);
				if (uuid == null) {
					uuid = "null";
				}else{
					uuid = uuid.replaceAll("[\r\n\"]", "");
					uuid = uuid.equals("") ? "null" : uuid;
				}
				operator = (String) map.get(operator_label);
				if (operator == null) {
					operator = "null";
				} else {
					operator = URLDecoder.decode(operator, "UTF-8");
					operator = operator.replaceAll("[\r\n\"]", "");
					operator = operator.equals("") ? "null" : operator;
				}
				network = (String) map.get(network_label);
				if (network == null) {
					network = "null";
				}else{
					network = network.replaceAll("[\r\n\"]", "");
					network = network.equals("") ? "null" : network;
				}
				brand = (String) map.get(brand_label);
				if (brand == null) {
					brand = "null";
				}else{
					brand = brand.replaceAll("[\r\n\"]", "");
					brand = brand.equals("") ? "null" : brand;
				}
				btype = (String) map.get(btype_label);
				if (btype == null) {
					btype = "null";
				} else {
					btype = URLDecoder.decode(btype, "UTF-8");
					btype = btype.replaceAll("[\r\n\"]", "");
					btype = btype.equals("") ? "null" : btype;
				}
				os = (String) map.get(os_label);
				if (os == null) {
					os = "null";
				}else{
					os = os.replaceAll("[\r\n\"]", "");
					os = os.equals("") ? "null" : os;
				}
				os_ver = (String) map.get(os_ver_label);
				if (os_ver == null) {
					os_ver = "N/A";
				}else{
					os_ver = os_ver.replaceAll("[\r\n\"]", "");
				}
				os_ver = os_ver.replaceAll("[^0-9.]", "");
				os_ver = os_ver.equals("") ? "N/A" : os_ver;
				
				wt = (String) map.get(wt_label);
				if (wt == null) {
					wt = "null";
				}else{
					wt = wt.replaceAll("[\r\n\"]", "");
					wt = wt.equals("") ? "null" : wt;
				}
				ht = (String) map.get(ht_label);
				if (ht == null) {
					ht = "null";
				}else{
					ht = ht.replaceAll("[\r\n\"]", "");
					ht = ht.equals("") ? "null" : ht;
				}
				
				guid = getGuid(mac, imei, deviceid, uuid);
				if(ndeviceid!=null && !ndeviceid.equals("")){
					guid2 = getGuid(mac, imei, ndeviceid, uuid);
				}
				if(guid == null){
					guid = "null";
				}
				
				if(guid2 == null){
					guid2 = "null";
				}
			} else {
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private String getGuid(String mac, String imei, String deviceid, String uuid) {
		try {
			MessageDigest md = (MessageDigest) MD5_DIGEST.clone();
			md.update((mac + "&" + imei + "&" + deviceid + "&" + uuid).getBytes("UTF-8"));
			byte[] res = md.digest();
			return byteToHexString(res);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (CloneNotSupportedException e) {
			e.printStackTrace();
		}

		return "";
	}

	private final String byteToHexString(byte[] src) {
		String s;
		char str[] = new char[16 * 2];
		int k = 0;
		for (int i = 0; i < 16; i++) {
			byte byte0 = src[i];
			str[k++] = hexDigits[byte0 >>> 4 & 0xf];
			str[k++] = hexDigits[byte0 & 0xf];
		}
		s = new String(str);
		return s;
	}

	private final static char hexDigits[] = { '0', '1', '2', '3', '4', '5',
			'6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };


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
		out.writeUTF(request_args);

		// args
		out.writeUTF(pid);
		out.writeUTF(ver);
		out.writeUTF(operator);
		out.writeUTF(network);
		out.writeUTF(brand);
		out.writeUTF(btype);
		out.writeUTF(os);
		out.writeUTF(os_ver);
		out.writeUTF(wt);
		out.writeUTF(ht);
		out.writeUTF(imei);
		out.writeUTF(imsi);
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
		ip = in.readUTF();
		date = in.readUTF();
		method = in.readUTF();
		uri = in.readUTF();
		response_code = in.readUTF();
		content_length = in.readUTF();
		request_time = in.readUTF();
		request_args = in.readUTF();

		// args
		pid = in.readUTF();
		ver = in.readUTF();
		operator = in.readUTF();
		network = in.readUTF();
		brand = in.readUTF();
		btype = in.readUTF();
		os = in.readUTF();
		os_ver = in.readUTF();
		wt = in.readUTF();
		ht = in.readUTF();
		imei = in.readUTF();
		imsi = in.readUTF();
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
	public int compareTo(guid_statis_request r) {
		if (r == null) {
			return 0;
		}
		return 0;
	}

	public String getRequest_body() {
		return request_body;
	}

	public void setRequest_body(String request_body) {
		this.request_body = request_body;
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

	public void setIp(String ip) {
		this.ip = ip;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public void setMethod(String method) {
		this.method = method;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public void setResponse_code(String response_code) {
		this.response_code = response_code;
	}

	public void setContent_length(String content_length) {
		this.content_length = content_length;
	}

	public void setRequest_time(String request_time) {
		this.request_time = request_time;
	}

	public void setUser_agent(String user_agent) {
		this.user_agent = user_agent;
	}

	public void setRequest_args(String request_args) {
		this.request_args = request_args;
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