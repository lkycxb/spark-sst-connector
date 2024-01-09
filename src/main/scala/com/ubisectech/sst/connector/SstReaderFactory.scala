/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.sst.connector


import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration
import org.rocksdb.{Options, ReadOptions, SstFileReader, SstFileReaderIterator}
import org.slf4j.LoggerFactory

import java.io.File

/**
 * A factory used to create Sst readers.
 *
 * @param sqlConf         SQL configuration.
 * @param broadcastedConf Broadcasted serializable Hadoop Configuration.
 * @param readDataSchema  Required schema in the batch scan.
 * @param partitionSchema Schema of partitions.
 * @param options         Options for reading a text file.
 * */
case class SstReaderFactory(
                             sqlConf: SQLConf,
                             broadcastedConf: Broadcast[SerializableConfiguration],
                             readDataSchema: StructType,
                             partitionSchema: StructType,
                             options: SstOptions) {
  private val LOGGER = LoggerFactory.getLogger(getClass)


  class SstReaderIter(iterator: SstFileReaderIterator) extends Iterator[InternalRow] {
    override def hasNext: Boolean = iterator.isValid

    override def next(): InternalRow = {
      val k = iterator.key()
      val v = iterator.value()
      iterator.next()
      val arr: Array[Any] = Array(k, v)
      new GenericInternalRow(arr)
    }
  }

  def builderIteratorReader(file: PartitionedFile): Iterator[InternalRow] = {
    val srcPath = file.toPath
    val pathStr = srcPath.toString

    LOGGER.info(s"load path:${srcPath}")
    val srcName = srcPath.getName
    val localSchema = "file://"
    val conf = broadcastedConf.value.value
    val readPath = if (!srcName.startsWith(localSchema)) {
      val localPath = conf.get("localPath")
      val baseName = FilenameUtils.getBaseName(srcPath.getName)
      val localPrefix = "reader_" + baseName + "_"
      val localFile = if (localPath == null || localPath.isEmpty) {
        File.createTempFile(localPrefix, ".tmp")
      } else {
        val dir = new File(localPath)
        if (!dir.exists()) {
          dir.mkdirs()
        }
        File.createTempFile(localPrefix, ".tmp", dir)
      }
      val localParent = localFile.getParentFile
      if (!localParent.exists()) {
        localParent.mkdirs()
      }
      val localAbsPath = localFile.getAbsolutePath
      val dstPath = new Path(localAbsPath)
      val dfs = srcPath.getFileSystem(conf)
      dfs.copyToLocalFile(false, srcPath, dstPath, true)
      localAbsPath
    } else {
      srcName.slice(localSchema.length, srcName.length)
    }
    val readEnv = new Options()
    val reader = new SstFileReader(readEnv)
    reader.open(readPath)
    reader.verifyChecksum
    val readOptions = new ReadOptions()
    val iterator = reader.newIterator(readOptions)
    iterator.seekToFirst
    val iter = new SstReaderIter(iterator)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => {
      reader.close()
      readOptions.close()
      readEnv.close()
      if (!srcName.startsWith(localSchema)) {
        val dfs = srcPath.getFileSystem(conf)
        dfs.delete(new Path(readPath), true)
      }
    }))
    iter
  }

  def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val iter = builderIteratorReader(file)
    val fileReader = new PartitionReaderFromIterator[InternalRow](iter)
    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }


}