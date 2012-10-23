package com.youku.wireless.guid;

import org.apache.hadoop.conf.Configuration;   
import org.apache.hadoop.io.Text;  
  
  
public class GuidOutputFormat extends MultipleOutputFormat<Text, Text> {  
      
    private final static String suffix = ".log";  
      
    //用key作为文件名
    @Override  
    protected String generateFileNameForKeyValue(Text key, Text value, Configuration conf) {  
        // TODO Auto-generated method stub  
    	//String dirpath = conf.get("hour");
        String path =  key.toString(); //文件的路径及名字 
        String filename = path.split(",")[0];
        //String[] dir = path.split("/");
        if(filename != null && !filename.equals("")){
        	filename = "part-"+filename.substring(filename.length()-1) ;
        }else{
        	filename = "part-null";
        }
        
        //int length = dir.length;   
        //String filename = dir[length -1];  
        //filename = filename.substring(0, filename.indexOf("_"));
        return filename + suffix;//输出的文件名
        //return null;
    }
  
      
}  
