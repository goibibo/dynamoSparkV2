/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied.  See the License for the
  * specific language governing permissions and limitations
  * under the License.
  *
  * Copyright © 2018 AudienceProject. All rights reserved.
  */
package com.goibibo.spark.datasourcev2.dynamoDB.connector

import java.util

import com.amazonaws.services.dynamodbv2.document._
import com.amazonaws.services.dynamodbv2.document.spec.{BatchWriteItemSpec, ScanSpec}
import com.amazonaws.services.dynamodbv2.model.{AttributeValue, ReturnConsumedCapacity, UpdateItemRequest}
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types._

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

private[dynamodb] class TableConnector(tableName: String, totalSegments: Int, options: DataSourceOptions)
    extends DynamoConnector with Serializable {

    private val consistentRead = options.getBoolean("stronglyConsistentReads", false)
    private val filterPushdown = options.getBoolean("filterPushdown", true)
    private val region = options.get("region").asScala
    private val roleArn = options.get("roleArn").asScala

    override val (keySchema, readLimit, itemLimit, totalSizeInBytes) = {
        val table = getDynamoDB(region, roleArn).getTable(tableName)
        val desc = table.describe()

        // Key schema.
        val keySchema = KeySchema.fromDescription(desc.getKeySchema.asScala)

        // Parameters.
        val bytesPerRCU = options.get("bytesPerRCU").asScala.getOrElse("4000").toInt
        val targetCapacity = options.get("targetCapacity").asScala.getOrElse("1").toDouble
        val readFactor = if (consistentRead) 1 else 2

        // Provisioned or on-demand throughput.
        val readThroughput = options.get("throughput").asScala.getOrElse(Option(desc.getProvisionedThroughput.getReadCapacityUnits)
          .filter(_ > 0).map(_.longValue().toString)
          .getOrElse("100")).toLong

        val writeThroughput = options.get("throughput").asScala.getOrElse(Option(desc.getProvisionedThroughput.getWriteCapacityUnits)
            .filter(_ > 0).map(_.longValue().toString)
            .getOrElse("100")).toLong

        // Rate limit calculation.
        val tableSize = desc.getTableSizeBytes
        val avgItemSize = tableSize.toDouble / desc.getItemCount
        val readCapacity = readThroughput * targetCapacity // targetCapacity = 1 or stronglyconsistant
        val writeCapacity = writeThroughput * targetCapacity

        val readLimit = readCapacity / totalSegments //per executor RCU
        val itemLimit = ((bytesPerRCU / avgItemSize * readLimit).toInt * readFactor) max 1

        val writeLimit = writeCapacity / totalSegments

        (keySchema, readLimit, itemLimit, tableSize.toLong)
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

        getDynamoDB(region, roleArn).getTable(tableName).scan(scanSpec)
    }

    private def mapValue(element: Any, elementType: DataType): Any = {
        elementType match {
            case ArrayType(innerType, _) => element.asInstanceOf[Seq[_]].map(e => mapValue(e, innerType)).asJava
            case MapType(keyType, valueType, _) =>
                if (keyType != StringType) throw new IllegalArgumentException(
                    s"Invalid Map key type '${keyType.typeName}'. DynamoDB only supports String as Map key type.")
                element.asInstanceOf[Map[String, _]].mapValues(e => mapValue(e, valueType)).asJava
            case StructType(fields) =>
                val row = element.asInstanceOf[Row]
                mapStruct(row, fields)
            case _ => element
        }
    }

    private def mapValueToAttributeValue(element: Any, elementType: DataType): AttributeValue = {
        elementType match {
            case ArrayType(innerType, _) => new AttributeValue().withL(element.asInstanceOf[Seq[_]].map(e => mapValueToAttributeValue(e, innerType)):_*)
            case MapType(keyType, valueType, _) =>
                if (keyType != StringType) throw new IllegalArgumentException(
                    s"Invalid Map key type '${keyType.typeName}'. DynamoDB only supports String as Map key type.")

                new AttributeValue().withM(element.asInstanceOf[Map[String, _]].mapValues(e => mapValueToAttributeValue(e, valueType)).asJava)

            case StructType(fields) =>
                val row = element.asInstanceOf[Row]
                new AttributeValue().withM( (fields.indices map { i =>
                    fields(i).name -> mapValueToAttributeValue(row(i), fields(i).dataType)
                }).toMap.asJava)
            case StringType => new AttributeValue().withS(element.asInstanceOf[String])
            case LongType | IntegerType | DoubleType | FloatType => new AttributeValue().withN(element.toString)
            case BooleanType => new AttributeValue().withBOOL(element.asInstanceOf[Boolean])
        }
    }

    private def mapStruct(row: Row, fields: Seq[StructField]): util.Map[String, Any] =
        (fields.indices map { i =>
            fields(i).name -> mapValue(row(i), fields(i).dataType)
        }).toMap.asJava

}
