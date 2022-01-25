/**
 * Copyright (c) 2016 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import site.ycsb.ByteIterator;
import site.ycsb.StringByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;
import java.io.*;
import java.util.Properties;

/**
 * YCSB binding for HDFS.
 */
public class HdfsClient extends DB {
  //-p option
  public static final String CONFIG_FILE_PROPERTY = "configfile";
  public static final String TEST_PATH_PROPERTY = "testpath";
  public static final String TEST_PATH_DEFAULT = "/";

  //hadoop config
  private Configuration conf = null;
  private FileSystem fs = null;
  private String testpath = "/";

  private boolean isInited = false;

  //Must Contain core-site.xml
  class NoHadoopConfigurationFileException extends Exception{
    NoHadoopConfigurationFileException() {
      super("\n-------------------------------------------------------------" +
            "\nThere is no path to the configuration file.                  " +
            "\nMust contain -p configfile=[path]/core-site.xml for Hadoop.  " +
            "\nRecommand :  -p configfile=/etc/hadoop/conf/core-site.xml    " +
            "\n-------------------------------------------------------------");
    }
    NoHadoopConfigurationFileException(String msg) {
      super(msg);
    }
  }

  public void init() throws DBException {
    Properties props = getProperties();

    //하둡 설정 경로
    String configfile = props.getProperty(CONFIG_FILE_PROPERTY);
    try {
      //no option
      if (configfile == null) {
        throw new NoHadoopConfigurationFileException();
      }

      //no file
      File f = new File(configfile);
      if(!f.isFile()) {
        throw new NoHadoopConfigurationFileException("Not Found " + configfile);
      }
    }catch (NoHadoopConfigurationFileException e) {
      e.printStackTrace();
      System.exit(1);
    }

    //워크로드가 들어갈 하둡 파일 시스템상의 경로
    testpath = props.getProperty(TEST_PATH_PROPERTY);
    if (testpath == null) {
      testpath = TEST_PATH_DEFAULT;
    }else if(testpath.charAt(testpath.length()-1) != '/') {
      testpath += "/";
    }

    try {
      //하둡 설정을 가져옴
      conf = new Configuration();
      conf.addResource(new Path(configfile));
      fs = FileSystem.get(conf);
    } catch (Exception e) {
      e.printStackTrace();
    }

    isInited = true;
  }

  public void cleanup() throws DBException {
    if (isInited) {
      conf = null;
      fs = null;
      isInited = false;
    }
  }

  //YCSB가 요청한 정보를 파일경로로 수정(table:dir, key:filename)
  private String getPath(String table, String key) {
    return testpath + table + "/" + key;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {

    FSDataInputStream in = null; //입력 스트림
    String filecontent = "";     //파일 전체 내용

    String filename = getPath(table, key);
    Path path = new Path(filename);

    try {
      //파일이 존재하지 않을 시 NOT FOUND
      if (!fs.exists(path)) {
        return Status.NOT_FOUND;
      }

      in = fs.open(path);         //스트림 연결
      filecontent = in.readUTF(); //읽기
      in.close();                 //스트림 닫기
    } catch (IOException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }

    //행 단위로 파싱
    String[] lines = filecontent.split("\n");
    //특정 필드가 없으면 모든 필드 읽기
    if (fields == null) {
      for (String line : lines) {
        String k = line.split(" ")[0];
        String v = line.split(" ")[1];
        result.put(k, new StringByteIterator(v));
      }
    } else {
      for (String line : lines) {
        String k = line.split(" ")[0];
        String v = line.split(" ")[1];

        //특정 필드만 읽기
        if (fields.contains(k)) {
          result.put(k, new StringByteIterator(v));
        }
      }
    }

    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    FSDataOutputStream out = null;

    String filename = getPath(table, key);
    Path path = new Path(filename);

    try {
      //기존파일 삭제
      if (fs.exists(path)) {
        fs.delete(path, true);
      }

      out = fs.create(path);
      for (Entry<String, ByteIterator> entry : values.entrySet()) {
        out.writeUTF(entry.getKey() + " " + entry.getValue().toString() + "\n");
      }
      out.close();

    } catch (IOException e) {
      e.printStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {

    String filename = getPath(table, key);
    Path path = new Path(filename);

    try {
      if (fs.exists(path)) {
        fs.delete(path, true);
      }
    } catch (IOException e){
      e.printStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }
}
