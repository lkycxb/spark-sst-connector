/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.sst.connector

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.types.{BinaryType, DataType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class SstTable(name: String,
                    sparkSession: SparkSession,
                    options: CaseInsensitiveStringMap,
                    paths: Seq[String],
                    userSpecifiedSchema: Option[StructType],
                    fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {



  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    SstScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    Some(SstUtils.getSchema)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new WriteBuilder {
      override def build(): Write = SstWrite(paths, formatName, supportsDataType, info)
    }

  override def supportsDataType(dataType: DataType): Boolean = dataType == BinaryType

  override def formatName: String = "sst"
}