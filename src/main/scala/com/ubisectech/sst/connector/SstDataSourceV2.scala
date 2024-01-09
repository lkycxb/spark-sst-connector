/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.sst.connector

import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class SstDataSourceV2 extends FileDataSourceV2 {


  //for DataSourceV2 and df.writer
  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[SstFileFormat]

  override def shortName(): String = "sst"

  //for no spec schema
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    SstTable(tableName, sparkSession, optionsWithoutPaths, paths, None, fallbackFileFormat)
  }
  //for spec schema
  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    SstTable(tableName, sparkSession, optionsWithoutPaths, paths, Some(schema), fallbackFileFormat)
  }
}