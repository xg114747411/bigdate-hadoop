package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient{

    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        printFiles("/");
    }

    public void printFiles(String path) throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop102:9820");
        Configuration conf = new Configuration();
        String user = "atguigu";
        FileSystem fs = FileSystem.get(uri , conf , user);
        FileStatus[] fss = fs.listStatus(new Path(path));
        for (FileStatus status : fss) {
            if(status.isFile()){
                //文件
                System.out.println("====FILE->"+status.getPath());
            }else{
                //目录
                System.out.println("DIR==="+ status.getPath());
                printFiles(status.getPath().toString());
            }
        }
        fs.close();
    }
    @Test
    public void testUploadFile() throws URISyntaxException, IOException, InterruptedException {
        URI uri = new URI("hdfs://hadoop102:9820");
        Configuration conf = new Configuration();
        conf.set("dfs.replication","5");
        String user = "atguigu";
        FileSystem fs = FileSystem.get(uri , conf , user);
        //参数1：是否删除源文件   参数2：是否覆盖目标文件  参数3：源文件路径   参数4：拷贝目标路径
        fs.copyFromLocalFile(false , true , new Path("D:/svnworkspace/test2") , new Path("/test"));
        fs.close();
    }
        @Test
        public void testMkdirs() throws IOException, InterruptedException, URISyntaxException {
            //1、通过namenode连接HDFS文件系统
            URI uri = new URI("hdfs://hadoop102:9820");//namenode连接地址
            Configuration conf = new Configuration();//配置
            String user = "atguigu";//操作hdfs的用户
            FileSystem fs = FileSystem.get(uri,conf,user);
            //2、创建目录
            fs.mkdirs(new Path("/test/hadoopclient1"));
            //3、关闭fs
            fs.close();
        }
}


