package com.netentsec.sparksql.framework;

import java.io.IOException;
import java.net.URI;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hdfs.DistributedFileSystem;
//import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.log4j.Logger;


public class HadoopOperation {
	
	/**
	 * 日志对象
	 */
	public static Logger logger = Logger.getLogger(HadoopOperation.class);
	
	public Configuration conf;
	
	private List<ArrayList<String>> queuen = new ArrayList<ArrayList<String>>();
	
	private int queue_size = 0;
	
	private String hdfs;
	
	private static String HADOOP_PATH;
	
	public HadoopOperation(){
		hdfs = HADOOP_PATH;
		for(int i = 0; i < 24; i ++){
			ArrayList<String> list = new ArrayList<String>();
			queuen.add(list);
		}
	}
	
	public int getQueue_size() {
		return queue_size;
	}

	public void setQueue_size() {
		this.queue_size ++;
	}

	public List<ArrayList<String>> getQueue(){
		return queuen;
	}

	/*
	 * upload the local file to the hds 
	 * 路径是全路径
	 */
	public void uploadLocalFile2HDFS(String s, String d)
	{
		Configuration conf = getConf();
		FileSystem fs;
		String data;
		try {
			//System.out.println(s + "===" + d);
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			fs = FileSystem.get(conf);
			//FileSystem hdfs = FileSystem.get(conf);
			Path src = new Path(s);
			Path dst = new Path(d);
			if(!fs.exists(dst)){
				fs.copyFromLocalFile(false, true, src, dst);
			}
			//fs.copyFromLocalFile(src, dst);
			//fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void writeHDFSFile(String filepath, String type, String filetime, int hour){
		Configuration conf = getConf();
		FileSystem fs;
		if(null == queuen.get(hour) || queuen.get(hour).isEmpty()){
			return;
		}
		try {
			conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			fs = FileSystem.get(URI.create(hdfs), conf);

			Date now = new Date();
			SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyyMMddHHmmss");//可以方便地修改日期格式
			DecimalFormat mFormat= new DecimalFormat("00");
			String current = datetimeFormat.format(now);
			String random4 = TimeUtil.Random4();
			if(fs.exists(new Path(filepath + "/" + type + "-" + filetime + mFormat.format(hour) + "0000-" + current + "-" + random4))){
				random4 = TimeUtil.Random4();
				FSDataOutputStream os = fs.create(new Path(filepath + "/" + type + "-" + filetime + mFormat.format(hour) + "0000-" + current + "-"  + random4));
				for(String data : queuen.get(hour)){
					os.write(data.getBytes("UTF-8"));
					//logger.warn("append:" + data);
				}
				os.flush();
				os.close();
				queuen.get(hour).clear();
			}else{
				FSDataOutputStream os = fs.create(new Path(filepath + "/" + type + "-" + filetime + mFormat.format(hour) + "0000-" + current + "-" + random4));
				for(String data : queuen.get(hour)){
					os.write(data.getBytes("UTF-8"));
					//logger.warn("new:" + data);
				}
				os.flush();
				os.close();
				
				queuen.get(hour).clear();
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("hdfs写入失败，失败数据写入本地，清空了写入数据！清空大小" + queuen.get(hour).size());
			queuen.get(hour).clear();
		}
	}
	
	/*
	 * delete the hdfs file 
	 * notice that the dst is the full path name
	 */
	public static boolean deleteHDFSFile(String dst) throws IOException
	{
		Configuration conf = getConf();
		FileSystem hdfs = FileSystem.get(conf);
		
		Path path = new Path(dst);
		boolean isDeleted = hdfs.delete(path);
		hdfs.close();
		return isDeleted;
	}
	
	
	/*
	 * read the hdfs file content
	 * notice that the dst is the full path name
	 */
	public static byte[] readHDFSFile(String dst) throws Exception
	{
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		
		// check if the file exists
		Path path = new Path(dst);
		if ( fs.exists(path) )
		{
			FSDataInputStream is = fs.open(path);
			// get the file info to create the buffer
			FileStatus stat = fs.getFileStatus(path);
			// create the buffer
			byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
		    is.readFully(0, buffer);
		    
		    is.close();
		    fs.close();
		    
		    return buffer;
		}
		else
		{
			throw new Exception("the file is not found .");
		}
	}
	
	
	/*
	 * make a new dir in the hdfs
	 * the dir may like '/tmp/testdir'
	 */
	public static void mkdir(String dir)
	{
		Configuration conf = getConf();
		try {
			FileSystem fs = FileSystem.get(conf);
			fs.mkdirs(new Path(dir));
			//fs.close();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/*
	 * delete a dir in the hdfs
	 * dir may like '/tmp/testdir'
	 */
	public static void deleteDir(String dir) throws IOException
	{
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(dir));
		fs.close();
	}
	
	//文件系统连接到 hdfs的配置信息 
	private static Configuration getConf(){
		Configuration conf = new Configuration();
		// 这句话很关键，这些信息就是hadoop配置文件中的信息
		//conf.set("mapred.job.tracker", "10.254.1.22:9001");
		conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
		conf.set("fs.hdfs.impl", 
		        org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		    );
		conf.set("fs.file.impl",
		        org.apache.hadoop.fs.LocalFileSystem.class.getName()
		    );
		conf.set("fs.default.name", HADOOP_PATH);
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER"); 
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable","true"); 
		conf.setInt("dfs.datanode.max.transfer.threads", 8192);
		return conf;
	}
	
	/**
	* @Title: listAll 
	* @Description: 列出目录下所有文件 
	* @return void    返回类型 
	* @throws
	 */
	public static void listAll(String dir) throws IOException
	{
		Configuration conf = getConf();
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(new Path(dir));
		for(int i = 0; i < stats.length; ++i)
		{
			if (!stats[i].isDir())
			{
				// regular file
				System.out.println(stats[i].getPath().toString());
			}
			else 
			{
				// dir
				System.out.println(stats[i].getPath().toString());
			}
//			else if(stats[i].())
//			{
//				// is s symlink in linux
//				System.out.println(stats[i].getPath().toString());
//			}
 				
		}
		fs.close();
	}
	
}