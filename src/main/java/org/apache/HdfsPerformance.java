package org.apache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsPerformance {
  private static long numFiles;
  private static long fileSizeInBytes = 128000000;
  private static long bufferSizeInBytes = 4000000;

  public static final Logger LOG = LoggerFactory.getLogger(HdfsPerformance.class);

  protected static long writeFile(String path, long fileSize, long bufferSize, int random) throws IOException {
    RandomAccessFile raf = null;
    long offset = 0;
    try {
      raf = new RandomAccessFile(path, "rw");
      while (offset < fileSize) {
        final long remaining = fileSize - offset;
        final long chunkSize = Math.min(remaining, bufferSize);
        byte[] buffer = new byte[(int)chunkSize];
        for (int i = 0; i < chunkSize; i ++) {
          buffer[i]= (byte) (i % random);
        }
        raf.write(buffer);
        offset += chunkSize;
      }
    } finally {
      if (raf != null) {
        raf.close();
      }
    }
    return offset;
  }

  private static void writeToHDFS(FileSystem fs, String srcPath,Path dstPath)
      throws IOException {
    long start = System.currentTimeMillis();
    FSDataOutputStream out = fs.create(dstPath);
    long end1 = System.currentTimeMillis();
    File file = new File(srcPath);
    FileInputStream in = new FileInputStream(file);
    IOUtils.copyBytes(in, out, 4096,true);
    long end2 = System.currentTimeMillis();
    out.close();
    long end3 = System.currentTimeMillis();
    System.out.println(srcPath+" meta cost: "+((end1-start)+(end3-end2)));
    System.out.println(srcPath+" write cost: "+(end2-end1));
    System.out.println(srcPath+" total cost: "+(end3-start));
  }

  public static void ReadFile(FileSystem fs, String hdfs) throws IOException {

    long start = System.currentTimeMillis();
    FSDataInputStream hdfsInStream = fs.open(new Path(hdfs));
    long end1 = System.currentTimeMillis();
    byte[] ioBuffer = new byte[1024];
    int readLen = hdfsInStream.read(ioBuffer);
    while(readLen!=-1) {
      readLen = hdfsInStream.read(ioBuffer);
    }
    hdfsInStream.close();
    long end2 = System.currentTimeMillis();
    System.out.println(hdfs+" meta cost: "+((end1-start)));
    System.out.println(hdfs+" read cost: "+(end2-end1));
    System.out.println(hdfs+" total cost: "+(end2-start));
    fs.close();
  }

  public static void main(String[] args) {
     try {
       String url = "hdfs://xxxxx:9000";
       Configuration conf = new Configuration();
       conf.set("fs.defaultFS", url);
       FileSystem fs = FileSystem.get(conf);

       if (args.length == 1) {
         ReadFile(fs, args[0]);
       } else {
         String path = "./"+ args[0];
         fileSizeInBytes = Integer.parseInt(args[1]);
         writeFile(path, fileSizeInBytes, bufferSizeInBytes, new Random().nextInt(127) + 1);
         String dstString = url+"/tmp/"+args[0]+System.currentTimeMillis();
         Path dstPath = new Path(dstString);
         writeToHDFS(fs, path, dstPath);
       }
       fs.close();
     } catch (Exception e) {
       System.err.println(e);
     }

  }
}
