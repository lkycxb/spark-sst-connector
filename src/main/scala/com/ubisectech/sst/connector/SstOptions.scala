/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.sst.connector


import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.{DataSourceOptions, FileSourceOptions}
import org.rocksdb.CompressionType

/**
 * Options for the Text data source.
 */
class SstOptions(@transient private val parameters: CaseInsensitiveMap[String])
  extends FileSourceOptions(parameters) {

  import SstOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  /**
   * Compression codec to use.
   */

  val createIfMissing = parameters.get(CREATEIFMISSING).getOrElse("true").toBoolean
  val compressionType = parameters.get(COMPRESSIONTYPE).map(CompressionType.valueOf).getOrElse(CompressionType.NO_COMPRESSION)
  val path = parameters.get(PATH)
  val localPath = parameters.get(LOCAL_PATH)

}

private[sst] object SstOptions extends DataSourceOptions {
  //CompressionType(CompressionType.DISABLE_COMPRESSION_OPTION);

  val CREATEIFMISSING = newOption("createIfMissing")
  val COMPRESSIONTYPE = newOption("compressionType")
  val PATH = newOption("path")
  val LOCAL_PATH = newOption("localPath")

}
