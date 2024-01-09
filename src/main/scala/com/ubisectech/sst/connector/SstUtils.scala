/*
 * Copyright (c) 2020 cb 2023
 * All rights reserved.
 * Website: https://www.ubisectech.com
 */

package com.ubisectech.sst.connector

import org.apache.spark.sql.types.{BinaryType, StructField, StructType}

object SstUtils {

  def getSchema: StructType = {
    val fields = Seq("key", "value")
    StructType(fields.map(d => StructField(d, BinaryType)))
  }
}
