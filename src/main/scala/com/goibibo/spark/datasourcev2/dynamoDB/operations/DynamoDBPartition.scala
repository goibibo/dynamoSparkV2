package com.goibibo.spark.datasourcev2.dynamoDB.operations

import com.goibibo.spark.datasourcev2.dynamoDB.connector.DynamoConnector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.InputPartition
import org.apache.spark.sql.types.StructType

class DynamoDBPartition(
                         schema: StructType,
                         partitionIndex: Int,
                         connector: DynamoConnector
                       ) extends InputPartition[InternalRow] {

  def createPartitionReader = new DynamoDBPartitionReader(schema, partitionIndex, connector)
}