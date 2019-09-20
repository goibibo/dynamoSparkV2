package com.goibibo.spark.datasourcev2.dynamoDB

import com.goibibo.spark.datasourcev2.dynamoDB.operations.DynamoDBReader
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport}

class DefaultSource extends DataSourceV2 with ReadSupport {
  def createReader(options: DataSourceOptions) = new DynamoDBReader(options)
}