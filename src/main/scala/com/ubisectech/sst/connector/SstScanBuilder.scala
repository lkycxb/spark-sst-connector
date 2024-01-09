/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.sst.connector

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

case class SstScanBuilder(
                           sparkSession: SparkSession,
                           fileIndex: PartitioningAwareFileIndex,
                           schema: StructType,
                           dataSchema: StructType,
                           options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) {


  override def build(): Scan = {
    SstScan(sparkSession, fileIndex, dataSchema, dataSchema, readPartitionSchema(), options,
      partitionFilters, dataFilters)
  }
}
