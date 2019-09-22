
package com.goibibo.spark.datasourcev2.dynamoDB.connector

import java.util.Optional

import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec
import com.amazonaws.services.dynamodbv2.document.{ItemCollection, ScanOutcome}
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import scala.compat.java8.OptionConverters._
import scala.collection.JavaConverters._

private[dynamodb] class TableIndexConnector(tableName: String, indexName: String, totalSegments: Int, options: DataSourceOptions)
    extends DynamoConnector with Serializable {

    private val consistentRead = options.getBoolean("stronglyConsistentReads", false)
    private val filterPushdown = options.getBoolean("filterPushdown", true)
    private val region = options.get("region").asScala
    private val roleArn = options.get("roleArn").asScala

    override val (keySchema, readLimit, itemLimit, totalSizeInBytes) = {
        val table = getDynamoDB(region, roleArn).getTable(tableName)
        val indexDesc = table.describe().getGlobalSecondaryIndexes.asScala.find(_.getIndexName == indexName).get

        // Key schema.
        val keySchema = KeySchema.fromDescription(indexDesc.getKeySchema.asScala)

        // Parameters.
        val bytesPerRCU = options.get("bytesPerRCU").asScala.getOrElse("4000").toInt
        val targetCapacity = options.get("targetCapacity").asScala.getOrElse("1").toDouble
        val readFactor = if (consistentRead) 1 else 2

        // Provisioned or on-demand throughput.
        val readThroughput = options.get("throughput").asScala.getOrElse(Option(indexDesc.getProvisionedThroughput.getReadCapacityUnits)
            .filter(_ > 0).map(_.longValue().toString)
            .getOrElse("100")).toLong

        // Rate limit calculation.
        val tableSize = indexDesc.getIndexSizeBytes
        val avgItemSize = tableSize.toDouble / indexDesc.getItemCount
        val readCapacity = readThroughput * targetCapacity

        val rateLimit = readCapacity / totalSegments
        val itemLimit = ((bytesPerRCU / avgItemSize * rateLimit).toInt * readFactor) max 1

        (keySchema, rateLimit, itemLimit, tableSize.toLong)
    }

    override def scan(segmentNum: Int, columns: Seq[String], filters: Seq[Filter]): ItemCollection[ScanOutcome] = {
        val scanSpec = new ScanSpec()
            .withSegment(segmentNum)
            .withTotalSegments(totalSegments)
            .withMaxPageSize(itemLimit)
            .withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
            .withConsistentRead(consistentRead)

        if (columns.nonEmpty) {
            val xspec = new ExpressionSpecBuilder().addProjections(columns: _*)

            if (filters.nonEmpty && filterPushdown) {
                xspec.withCondition(FilterPushdown(filters))
            }

            scanSpec.withExpressionSpec(xspec.buildForScan())
        }

        getDynamoDB(region, roleArn).getTable(tableName).getIndex(indexName).scan(scanSpec)
    }

}
