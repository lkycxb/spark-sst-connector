/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.sst

import org.apache.spark.sql.{DataFrame, DataFrameReader, DataFrameWriter}
import org.apache.spark.util.SerializableConfiguration

package object connector {

  /**
   * spark reader for nebula graph
   */
  implicit class SstFromPathByDataFrameReader(reader: DataFrameReader) {
    def fromSst(): DataFrame = {
      reader.format(classOf[SstDataSourceV2].getName).load()
    }
  }

  implicit class SstToPathByDataFrameWriter[T](writer: DataFrameWriter[T]) {
    def toSst(path: String): Unit = {
      writer.format(classOf[SstDataSourceV2].getName).save(path)
    }
  }

  implicit class SstToPathByDataFrame(df: DataFrame) {
    def toSst(path: String): Unit = {
      val sstOptions = new SstOptions(Map.empty[String, String])
      val hadoopConf = df.sparkSession.sparkContext.hadoopConfiguration
      val broadcastedConf = df.sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
      df.rdd.foreachPartition(iter => {
        val writer = new SstOutputWriterForRdd(path = path, sstOptions = sstOptions, broadcastedConf)
        iter.foreach(row => {
          val k = row.getAs[Array[Byte]](0)
          val v = row.getAs[Array[Byte]](1)
          writer.write(k, v)
        })
        writer.close()
      })
    }
  }
}
