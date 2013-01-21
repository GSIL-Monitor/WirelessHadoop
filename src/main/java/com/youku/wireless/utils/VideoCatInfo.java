package com.youku.wireless.utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class VideoCatInfo {
	public Map<String, String> videocatMap = new HashMap<String, String>();

	public VideoCatInfo(String vidata) {
		loadVideoFile(vidata);
	}

	public VideoCatInfo(Reader reader) {
		loadVideoFile(reader);
	}

	public VideoCatInfo(InputStream in) {
		loadVideoFile(in);
	}

	private void loadVideoFile(InputStream in) {
		InputStreamReader reader = new InputStreamReader(in);
		loadVideoFile(reader);
	}

	private void loadVideoFile(String vidata) {
		try {
			loadVideoFile(new FileReader(vidata));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("video data file:[" + vidata + "] not found");
		}
	}

	private void loadVideoFile(Reader reader) {
		try {
			//List<Catinfo> catList = new ArrayList<Catinfo>();
			BufferedReader br = null;
			br = new BufferedReader(reader);
			String line = null;

			while ((line = br.readLine()) != null) {
				// StringTokenizer itr = new StringTokenizer(line, "\t,");
				String[] catinfo = line.split("\t");

				if (catinfo.length > 13) {
					//Catinfo cat = new Catinfo(catinfo[0], catinfo[12]);
					//catList.add(cat);
					videocatMap.put(catinfo[0], catinfo[12]);
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public class Catinfo {
		String vid = null;
		String cid = null;

		public Catinfo() {

		}

		public Catinfo(String vid, String catid) {
			this.vid = vid;
			this.cid = catid;
		}
	}
}
