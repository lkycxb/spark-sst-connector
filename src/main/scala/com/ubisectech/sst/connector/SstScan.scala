/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.sst.connector

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

import scala.collection.JavaConverters._

case class SstScan(
                    sparkSession: SparkSession,
                    fileIndex: PartitioningAwareFileIndex,
                    dataSchema: StructType,
                    readDataSchema: StructType,
                    readPartitionSchema: StructType,
                    options: CaseInsensitiveStringMap,
                    partitionFilters: Seq[Expression] = Seq.empty,
                    dataFilters: Seq[Expression] = Seq.empty)
  extends TextBasedFileScan(sparkSession, options) {

  private val optionsAsScala = options.asScala.toMap
  private lazy val sstOptions = new SstOptions(optionsAsScala)

  override def isSplitable(path: Path): Boolean = {
    false
  }

  override def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplitable(path))
    "the sst datasource not splittable"
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    //assert( readDataSchema.length <= 1, "Sst data source only produces a single data column named 'value'.")
    val hadoopConf = {
      val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
      // Hadoop Configurations are case sensitive.
      sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    }
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    SstPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf, dataSchema,
      readPartitionSchema, sstOptions)
  }

  override def equals(obj: Any): Boolean = obj match {
    case t: SstScan => super.equals(t) && options == t.options
    case _ => false
  }

  override def hashCode(): Int = super.hashCode()

}
