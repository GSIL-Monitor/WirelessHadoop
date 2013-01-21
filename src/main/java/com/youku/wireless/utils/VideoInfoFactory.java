package com.youku.wireless.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class VideoInfoFactory {
	private static final String VIDEO_FILE_PATH = "/commons/yvideo/db_t_video_info/*";

	public static VideoCatInfo makeVideoCat(Configuration conf) throws IOException {
		long time = 0L;
		try {
			return makeVideoInfo(conf, VIDEO_FILE_PATH);
		} catch (IOException e) {
			if (time < 120000L) {
				try {
					Thread.sleep(10000L);
				} catch (InterruptedException localInterruptedException) {
				}
				time += 10000L;
			}
			throw e;
		}
	}

	public static VideoCatInfo makeVideoInfo(Configuration conf, String hdfsPath)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream input = fs.open(new Path(hdfsPath));
		VideoCatInfo videos = new VideoCatInfo(input);
		return videos;
	}

}
