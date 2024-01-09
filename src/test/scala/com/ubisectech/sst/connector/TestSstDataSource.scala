/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.sst.connector

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import junit.framework.TestCase
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

class TestSstDataSource extends TestCase {

  override def setUp() = {
    System.setProperty("hadoop.home.dir", "")
  }

  def testJson(): Unit = {
    val objectMapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val seq = Seq("F:\\share\\vulncomponents\\nebula\\compograph\\sst2\\testtag\\1\\1-62-1.sst", "F:\\share\\vulncomponents\\nebula\\compograph\\sst2\\testtag\\2\\2-63-1.sst")
    val str = objectMapper.writeValueAsString(seq)
    println(str)
    val strSeq = objectMapper.readValue(str, classOf[Seq[String]])
    strSeq.foreach(println)
    //    val str1="[\"F:\\share\\vulncomponents\\nebula\\compograph\\sst2\\testtag\\1\\1-62-1.sst\",\"F:\\share\\vulncomponents\\nebula\\compograph\\sst2\\testtag\\2\\2-63-1.sst\"]"
    val str1 = "[\"f:/abc.sst\",\"def.sst\"]"
    val strSeq1 = objectMapper.readValue(str1, classOf[Seq[String]])
    strSeq1.foreach(println)
    //    val str2 = "[f:/abc.sst,def.sst]"
    //    val strSeq2 = objectMapper.readValue(str2, classOf[Seq[String]])
    //    strSeq2.foreach(println)
  }

  def testReaderAndWriter(): Unit = {
    val conf = new SparkConf()
    conf.setAppName("sst")
    conf.setMaster("local[1]")
    //for hadoop of windows
    val isWin = System.getProperty("os.name").toLowerCase().contains("win")
    if (isWin) {
      conf.set("spark.hadoop.fs.file.impl", classOf[BareLocalFileSystem].getName)
    }
    val spark = SparkSession.builder().config(conf).getOrCreate()
    //    spark.sparkContext.hadoopConfiguration.setClass("fs.file.impl", classOf[BareLocalFileSystem], classOf[FileSystem])

    //    val paths="[\"F:\\share\\vulncomponents\\nebula\\compograph\\sst2\\testtag\\1\\1-62-1.sst\",\"F:\\share\\vulncomponents\\nebula\\compograph\\sst2\\testtag\\2\\2-63-1.sst\"]"
    //      .replace("\\","/")
    //    val paths = "[\"F:\\share\\vulncomponents\\nebula\\compograph\\temp\\savetxt\\data_3.sst\"]"
    //      .replace("\\", "/")
    val paths = "F:\\share\\vulncomponents\\nebula\\compograph\\sst2\\testtag\\1\\1-62-1.sst" .replace("\\", "/")
    val localPaths = "F:\\share\\vulncomponents\\nebula\\compograph\\temp"
      .replace("\\", "/")
    println("localPaths:" + localPaths)
    val schema = StructType(Seq(StructField("key", BinaryType), StructField("value", BinaryType)))
    val df = spark.read
      //      .option("paths", paths)
      .option("path", paths)
      .schema(schema)
      .fromSst()
      .sortWithinPartitions("key")
    val count = df.count()
    df.printSchema()
    df.show(10)

    val savePathStr = "file:///F:\\share\\vulncomponents\\nebula\\compograph\\temp\\savetxt1"
    val savePath = new Path(savePathStr)
    val dfs = savePath.getFileSystem(df.sparkSession.sparkContext.hadoopConfiguration)
    if (dfs.exists(savePath)) {
      dfs.delete(savePath, true)
    }
    df.write.toSst(savePathStr)

    //df.toSst(savePathStr)
    spark.stop()
    println(s"count:${count}")


  }

}
