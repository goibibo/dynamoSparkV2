package com.goibibo.spark.datasourcev2.dynamoDB.operations


import com.goibibo.spark.datasourcev2.dynamoDB.connector.DynamoConnector
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{InputPartition, InputPartitionReader}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity
import com.google.common.util.concurrent.RateLimiter
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.{StructField, StructType}

private[dynamodb] class DynamoDBPartitionReader(schema: StructType,
                                            partitionIndex: Int,
                                            connector: DynamoConnector)
  extends InputPartitionReader[InternalRow] with Serializable {

  @transient
  private lazy val typeConversions = schema.collect({
    case StructField(name, dataType, _, _) => name -> TypeConversion(name, dataType)
  }).toMap

  def scanTable(requiredColumns: Seq[String], filters: Seq[Filter]): Iterator[Row] = {

    if (connector.isEmpty) return Iterator.empty

    val rateLimiter = RateLimiter.create(connector.readLimit)

    val scanResult = connector.scan(index, requiredColumns, filters)

    val pageIterator = scanResult.pages().iterator().asScala

    new Iterator[Row] {

      var innerIterator: Iterator[Row] = Iterator.empty
      var prevConsumedCapacity: Option[ConsumedCapacity] = None

      @tailrec
      override def hasNext: Boolean = innerIterator.hasNext || {
        if (pageIterator.hasNext) {
          nextPage()
          hasNext
        }
        else false
      }

      override def next(): Row = innerIterator.next()

      private def nextPage(): Unit = {
        // Limit throughput to provisioned capacity.
        prevConsumedCapacity
          .map(capacity => math.ceil(capacity.getCapacityUnits).toInt)
          .foreach(rateLimiter.acquire)

        val page = pageIterator.next()
        prevConsumedCapacity = Option(page.getLowLevelResult.getScanResult.getConsumedCapacity)
        innerIterator = page.getLowLevelResult.getItems.iterator().asScala.map(itemToRow(requiredColumns))
      }

    }
  }

  private def itemToRow(requiredColumns: Seq[String])(item: Item): Row =
    if (requiredColumns.nonEmpty) Row.fromSeq(requiredColumns.map(columnName => typeConversions(columnName)(item)))
    else Row.fromSeq(item.asMap().asScala.values.toSeq.map(_.toString))

  override def index: Int = partitionIndex

}
