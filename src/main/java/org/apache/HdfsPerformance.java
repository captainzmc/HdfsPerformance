package org.apache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileInputStream;
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsPerformance {
  private static long numFiles;
  private static long fileSizeInBytes = 128000000;
  private static long bufferSizeInBytes = 4000000;
  private static long maxThreadNum = 1000;
  private static String[] storageDir = new String[]{"/data/ratis/", "/data1/ratis/", "/data2/ratis/", "/data3/ratis/",
      "/data4/ratis/", "/data5/ratis/", "/data6/ratis/", "/data7/ratis/", "/data8/ratis/", "/data9/ratis/",
      "/data10/ratis/", "/data11/ratis/"};

  public static final Logger LOG = LoggerFactory.getLogger(HdfsPerformance.class);

  private static void createDirs() throws IOException {
    for (String dir : storageDir) {
      Files.createDirectory(new File(dir).toPath());
    }
  }

  public static String getPath(String fileName) {
    int hash = fileName.hashCode() % storageDir.length;
    return new File(storageDir[Math.abs(hash)], fileName).getAbsolutePath();
  }

  protected static void dropCache() throws InterruptedException, IOException {
    String[] cmds = {"/bin/sh","-c","echo 3 > /proc/sys/vm/drop_caches"};
    Process pro = Runtime.getRuntime().exec(cmds);
    pro.waitFor();
  }

  private static CompletableFuture<Long> writeFileAsync(String path, ExecutorService executor) {
    final CompletableFuture<Long> future = new CompletableFuture<>();
    CompletableFuture.supplyAsync(() -> {
      try {
        future.complete(
            writeFile(path, fileSizeInBytes, bufferSizeInBytes, new Random().nextInt(127) + 1));
      } catch (IOException e) {
        future.completeExceptionally(e);
      }
      return future;
    }, executor);
    return future;
  }

  protected static List<String> generateFiles(ExecutorService executor) {
    UUID uuid = UUID.randomUUID();
    List<String> paths = new ArrayList<>();
    List<CompletableFuture<Long>> futures = new ArrayList<>();
    for (int i = 0; i < numFiles; i ++) {
      String path = getPath("file-" + uuid + "-" + i);
      paths.add(path);
      futures.add(writeFileAsync(path, executor));
    }

    for (int i = 0; i < futures.size(); i ++) {
      long size = futures.get(i).join();
      if (size != fileSizeInBytes) {
        System.err.println("Error: path:" + paths.get(i) + " write:" + size +
            " mismatch expected size:" + fileSizeInBytes);
      }
    }

    return paths;
  }

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

  private static Map<String, CompletableFuture<Boolean>> writeByHeapByteBuffer(
      List<String> paths, List<FSDataOutputStream> outs, ExecutorService executor) {
    Map<String, CompletableFuture<Boolean>> fileMap = new HashMap<>();

    for(int i = 0; i < paths.size(); i ++) {
      String path = paths.get(i);
      FSDataOutputStream out = outs.get(i);
      final CompletableFuture<Boolean> future = new CompletableFuture<>();
      CompletableFuture.supplyAsync(() -> {
        File file = new File(path);
        try (FileInputStream fis = new FileInputStream(file)) {
          int bytesToRead = (int)bufferSizeInBytes;
          byte[] buffer = new byte[bytesToRead];
          long offset = 0L;
          while(fis.read(buffer, 0, bytesToRead) > 0) {
            out.write(buffer);
            offset += bytesToRead;
            bytesToRead = (int) Math.min(fileSizeInBytes - offset, bufferSizeInBytes);
            if (bytesToRead > 0) {
              buffer = new byte[bytesToRead];
            }
          }
          future.complete(true);
        } catch (Throwable e) {
          LOG.error("exception", e);
          future.complete(false);
        }

        return future;
      }, executor);

      fileMap.put(path, future);
    }
    return fileMap;
  }

  public static int getNumThread() {
    return (int)(numFiles < maxThreadNum ? numFiles : maxThreadNum);
  }

  public static void main(String[] args) {
     try {
       numFiles = Integer.parseInt(args[0]);
       fileSizeInBytes = Integer.parseInt(args[1]);
       bufferSizeInBytes = Integer.parseInt(args[2]);
       System.out.println("numFiles:" + numFiles);
       createDirs();
       final ExecutorService executor = Executors.newFixedThreadPool(getNumThread());

       List<String> paths = generateFiles(executor);
       dropCache();

       Configuration conf = new Configuration();
       conf.set("fs.defaultFS", "hdfs://localhost:9000");
       conf.setInt("dfs.replication", 3);
       //conf.setInt("dfs.replication", 1);
       FileSystem fs = FileSystem.get(conf);

       List<FSDataOutputStream> outs = new ArrayList<>();
       for (int i = 0; i < paths.size(); i ++) {
         Path dstPath = new Path(paths.get(i)); //目标路径
         //打开一个输出流
         FSDataOutputStream outputStream = fs.create(dstPath);
         outs.add(outputStream);
       }

       System.out.println("Start write now");
       long start = System.currentTimeMillis();

       // Write key with random name.
       Map<String, CompletableFuture<Boolean>> map = writeByHeapByteBuffer(paths, outs, executor);

       for (String path : map.keySet()) {
         CompletableFuture<Boolean> future = map.get(path);
         if (!future.join().booleanValue()) {
           System.err.println("Error: path:" + path + " write fail");
         }
       }

       long end = System.currentTimeMillis();
       System.err.println("cost:" + (end - start) + " xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
     } catch (Exception e) {
       System.err.println(e);
     }

    System.out.println("HDFS Demo End.");
    System.exit(0);
  }
}
