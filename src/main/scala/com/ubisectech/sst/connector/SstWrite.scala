/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.sst.connector

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.v2.FileWrite
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

case class SstWrite(
                     paths: Seq[String],
                     formatName: String,
                     supportsDataType: DataType => Boolean,
                     info: LogicalWriteInfo) extends FileWrite {
  private def verifySchema(schema: StructType): Unit = {
    if (schema.size != 2) {
      throw new AnalysisException(
        errorClass = "_LEGACY_ERROR_TEMP_1290",
        messageParameters = Map("schemaSize" -> schema.size.toString))
    }
  }

  override def prepareWrite(
                             sqlConf: SQLConf,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {
    verifySchema(dataSchema)
    val sstOptions = new SstOptions(options)
    new OutputWriterFactory {
      override def newInstance(
                                path: String,
                                dataSchema: StructType,
                                context: TaskAttemptContext): OutputWriter = {
        new SstOutputWriterForTable(path, dataSchema, sstOptions, context)
      }

      override def getFileExtension(context: TaskAttemptContext): String = {
        ".sst"
      }
    }
  }
}

