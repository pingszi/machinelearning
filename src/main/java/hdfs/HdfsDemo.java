package hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;

/**
  *********************************************************
  ** @desc  ：hdfs上传、下载、删除等常用命令操作                                          
  ** @author  Pings                                     
  ** @date    2017-09-11
  ** @version v1.0                                                                               
  * *******************************************************
  */
public class HdfsDemo {

    /**
     *********************************************************
     ** @desc ：初始化                                             
     ** @author Pings                                     
     ** @date   2017-09-11                                                                                  
     * *******************************************************
     */
    public void init() throws Exception {
        FileSystem fs = FileSystem.get(new URI("hdfs://pings001"), new Configuration(), "hdfs");
    }

    /**
     *********************************************************
     ** @desc ：删除文件                                            
     ** @author Pings                                     
     ** @date   2017-09-11                                                                                  
     * *******************************************************
     */
    public void del() throws IOException {
        boolean flag = this.fs.delete(new Path("/hadoopinput/wordcount/hadoopWordCount.jar"), true);
    }

    /**
     *********************************************************
     ** @desc ：创建文件夹                                             
     ** @author Pings                                     
     ** @date   2017-09-11                                                                                  
     * *******************************************************
     */
    public void mkdir() throws IOException {
        boolean flag = this.fs.mkdirs(new Path("/hadoopinput"));
    }

    /**
     *********************************************************
     ** @desc ：上传文件                                            
     ** @author Pings                                     
     ** @date   2017-09-11                                                                                  
     * *******************************************************
     */
    public void upload() throws IOException {
        String file = "/Users/apple/Documents/project/hadoop/hadoopWordCount/src/main/java/mapreduce/dc/HTTP_20130313143750.dat";
        FSDataOutputStream out = fs.create(new Path("/hadoopinput/dc/data.dat"));
        FileInputStream in = new FileInputStream(new File(file));
        IOUtils.copyBytes(in, out, 2048, true);
    }

    /**
     *********************************************************
     ** @desc ：下载文件                                             
     ** @author Pings                                     
     ** @date   2017-09-11                                                                                  
     * *******************************************************
     */
    public void download() throws IOException {
        InputStream in = this.fs.open(new Path("/hadoopinput/file.pdf"));
        FileOutputStream out = new FileOutputStream(new File("/Users/apple/Downloads/file.pdf"));
        IOUtils.copyBytes(in, out, 2048, true);
    }
}
