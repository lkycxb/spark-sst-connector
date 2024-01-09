/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.sst.connector


import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

/**
 * A factory used to create Sst readers.
 *
 * @param sqlConf         SQL configuration.
 * @param broadcastedConf Broadcasted serializable Hadoop Configuration.
 * @param readDataSchema  Required schema in the batch scan.
 * @param partitionSchema Schema of partitions.
 * @param options         Options for reading a text file.
 * */
case class SstPartitionReaderFactory(
                                      sqlConf: SQLConf,
                                      broadcastedConf: Broadcast[SerializableConfiguration],
                                      readDataSchema: StructType,
                                      partitionSchema: StructType,
                                      options: SstOptions) extends FilePartitionReaderFactory {


  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val iterator = SstReaderFactory(sqlConf, broadcastedConf, readDataSchema,
      partitionSchema, options).builderIteratorReader(file)
    val fileReader = new PartitionReaderFromIterator[InternalRow](iterator)
    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }
}