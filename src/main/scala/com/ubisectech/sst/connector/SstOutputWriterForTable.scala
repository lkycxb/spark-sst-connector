/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.sst.connector


import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import org.rocksdb.{EnvOptions, Options, RocksDB, SstFileWriter}
import org.slf4j.LoggerFactory

import java.io.File

class SstOutputWriterForTable(
                       val path: String,
                       dataSchema: StructType,
                       sstOptions: SstOptions,
                       context: TaskAttemptContext)
  extends OutputWriter {
  private val LOGGER = LoggerFactory.getLogger(getClass)
  private var writeOptions: Options = _
  private var writeEnv: EnvOptions = _
  private var localTmpPath: String = _

  private lazy val writer = {
    try {
      RocksDB.loadLibrary()
      LOGGER.info("Loading RocksDB successfully")
    } catch {
      case _: Exception =>
        LOGGER.error("Can't load RocksDB library!")
    }

    writeEnv = new EnvOptions()
    writeOptions = new Options()
    writeOptions.setCreateIfMissing(sstOptions.createIfMissing)
    writeOptions.setCompressionType(sstOptions.compressionType)

    val w = new SstFileWriter(writeEnv, writeOptions);

    val srcPath = new Path(path)
    val localPrefix = "reader_" + srcPath.getName + "_"
    val localFile = if (sstOptions.localPath.isDefined) {
      File.createTempFile(localPrefix, ".sst", new File(sstOptions.localPath.get))
    } else {
      File.createTempFile(localPrefix, ".sst")
    }
    val localParent = localFile.getParentFile
    if (!localParent.exists()) {
      localParent.mkdirs()
    }
    localTmpPath = localFile.getAbsolutePath
    w.open(localTmpPath)
    w
  }


  override def write(row: InternalRow): Unit = {
    val k = row.getBinary(0)
    val v = row.getBinary(1)
    writer.put(k, v)
  }

  override def close(): Unit = {
    writer.finish()
    writer.close()
    writeOptions.close()
    writeEnv.close()
    if (localTmpPath != null) {
      val conf = context.getConfiguration
      val srcPath = new Path(localTmpPath)
      val srcDfs = srcPath.getFileSystem(conf)
      val dstPath = new Path(path)
      srcDfs.copyFromLocalFile(true, true, srcPath, dstPath)
    }
  }
}
