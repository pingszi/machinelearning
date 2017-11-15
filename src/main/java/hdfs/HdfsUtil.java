package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
  *********************************************************
  ** @desc  ：hdfs常用命令                                     
  ** @author  Pings                                     
  ** @date    2017-09-11
  ** @version v1.1
  **
  ** @update  v1.1  增加设置连接信息功能（setConnectInfo）                                                                             
  * *******************************************************
  */
public class HdfsUtil {

    //**连接hdfs的用户名(与hdfs的用户同名，避免远程连接的权限问题)
    private static String DEFAULT_USER = "hdfs";
    //**hdfs连接地址
    private static String DEFAULT_URL = "hdfs://ping001";

    /**
     *********************************************************
     ** @desc ： 设置hdfs的连接信息                                          
     ** @author Pings     
     ** @param  url  hdfs连接地址 
     ** @param  user 连接hdfs的用户名(与hdfs的用户同名，避免远程连接的权限问题)                                
     ** @date   2017-11-15                                                                                  
     * *******************************************************
     */
    public static void setConnectInfo(String url, String user) {
        DEFAULT_URL = url;
        DEFAULT_USER = user;
    }

    /**
     *********************************************************
     ** @desc ：连接默认的hdfs文件系统                                              
     ** @author Pings                                     
     ** @date   2017-09-11                                                                                  
     * *******************************************************
     */
    public static FileSystem getFileSystem() {
        return getFileSystem(DEFAULT_URL, DEFAULT_USER);
    }

    /**
     *********************************************************
     ** @desc ：连接指定的hdfs文件系统                                               
     ** @author Pings    
     ** @param  url  hdfs连接地址 
     ** @param  user 连接hdfs的用户名(与hdfs的用户同名，避免远程连接的权限问题)                                   
     ** @date   2017-09-11                                                                                  
     * *******************************************************
     */
    private static FileSystem getFileSystem(String url, String user) {
        try {
            if(user != null && !user.trim().equals(""))
                return FileSystem.get(new URI(url), new Configuration(), user);
            else
                return FileSystem.get(new URI(url), new Configuration());
        } catch (IOException | InterruptedException | URISyntaxException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     *********************************************************
     ** @desc ：查看目录下的文件                                           
     ** @author Pings   
     ** @param  dir  需要查看的目录                         
     ** @date   2017-09-11                                                                                  
     * *******************************************************
     */
    public static String[] listFiles(String dir) {
        try(FileSystem fs = getFileSystem()) {
            FileStatus[] status = fs.listStatus(new Path(dir));
            Path[] paths = FileUtil.stat2Paths(status);

            return (String[])Arrays.stream(paths).map(path -> path.toString()).toArray();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    /**
     *********************************************************
     ** @desc ：删除文件                                            
     ** @author Pings   
     ** @param  dir  需要删除的文件或目录                           
     ** @date   2017-09-11                                                                                  
     * *******************************************************
     */
    public static boolean del(String dir)  {
        boolean flag = false;

        try(FileSystem fs = getFileSystem()) {
            flag = fs.delete(new Path(dir), true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return flag;
    }

    /**
     *********************************************************
     ** @desc ：创建文件夹                                             
     ** @author Pings   
     ** @param  dir  需要创建的文件夹                               
     ** @date   2017-09-11                                                                                  
     * *******************************************************
     */
    public static boolean mkdirs(String dir) throws IOException {
        boolean flag = false;

        try(FileSystem fs = getFileSystem()) {
            flag = fs.mkdirs(new Path(dir));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return flag;
    }

    /**
     *********************************************************
     ** @desc ：上传文件                                            
     ** @author Pings  
     ** @param  source  源文件(本地文件)
     ** @param  target  目标文件(hdfs上的文件)                                      
     ** @date   2017-09-11                                                                                  
     * *******************************************************
     */
    public static void upload(String source, String target) {
        try(FileSystem fs = getFileSystem()) {
            fs.copyFromLocalFile(new Path(source), new Path(target));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *********************************************************
     ** @desc ：下载文件                                             
     ** @author Pings  
     ** @param  source  源文件(hdfs上的文件)
     ** @param  target  目标文件(本地文件)                                    
     ** @date   2017-09-11                                                                                  
     * *******************************************************
     */
    public static void download(String source, String target) {
        try(FileSystem fs = getFileSystem()) {
            fs.copyToLocalFile(new Path(source), new Path(target));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
