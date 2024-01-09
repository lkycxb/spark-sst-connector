/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.sst.connector

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriter, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{BinaryType, DataType, StructType}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.SerializableConfiguration

/**
 * for DataSourceV2 and df.writer
 */
class SstFileFormat extends FileFormat
  with DataSourceRegister
  with Logging
  with Serializable {


  override def shortName(): String = "sst"

  override def toString: String = "Sst"

  private def verifySchema(schema: StructType): Unit = {
    if (schema.size != 2) {
      throw new AnalysisException(
        errorClass = "_LEGACY_ERROR_TEMP_1290",
        messageParameters = Map("schemaSize" -> schema.size.toString))
    }
  }

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = {
    false
  }

  override def inferSchema(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = Some(SstUtils.getSchema)

  override def prepareWrite(
                             sparkSession: SparkSession,
                             job: Job,
                             options: Map[String, String],
                             dataSchema: StructType): OutputWriterFactory = {
    verifySchema(dataSchema)
    val sstOptions = new SstOptions(options)
    //val conf = job.getConfiguration

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


  override def buildReader(
                            sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    assert(
      requiredSchema.length <= 1,
      "Sst data source only produces a single data column named \"value\".")
    val sstOptions = new SstOptions(options)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))
    (file: PartitionedFile) => {
      val iterator = SstReaderFactory(sparkSession.sessionState.conf, broadcastedHadoopConf, dataSchema,
        partitionSchema, sstOptions).builderIteratorReader(file)
      iterator
    }
  }


  override def hashCode(): Int = getClass.hashCode()

  override def equals(other: Any): Boolean = other.isInstanceOf[SstFileFormat]

  override def supportDataType(dataType: DataType): Boolean = dataType match {
    case _: BinaryType => true
    case _ => false
  }
}
