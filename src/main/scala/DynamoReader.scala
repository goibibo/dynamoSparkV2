package com.goibibo.spark.datasourcev2.dynamoDB

// scala imports
import scala.collection.JavaConverters._

// spark imports
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

//java imports
import java.util.ArrayList
import java.util.Arrays
import java.util.HashMap
import java.util.Map
import java.util.List
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

// aws imports
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.DeleteTableRequest
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.model.PutItemRequest
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.ScanResult
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.amazonaws.services.dynamodbv2.model.TableStatus
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.document.ItemUtils
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder

// other orgs imports
import org.json4s._
import org.json4s.jackson.JsonMethods._







case class Attribute(
                      attributeName: String,
                      attributeDataType: String,
                      attributeSubDataType: String
                    )
case class Schema(
                   attributesCount: Int,
                   attributes: Seq[Attribute]
                 )
// Segment class
case class SegmentDetails(
                           tableName: String,
                           region: String,
                           itemLimit: Int,
                           totalSegments: Int,
                           segment: Int
                         )


object SchemaConverter {
  val dynamoDBSparkMapping = Map(
    "Number" ->  DoubleType,
    "String" -> StringType,
    "Binary" -> BinaryType,
    "Boolean" -> BooleanType,
    "Null" -> StringType,
    "List" -> StringType,
    "Map" ->  StringType,
    "Set" -> StringType
  ).withDefaultValue(StringType)

  def createSparkContext(appName: String) = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()
    spark
  }

  def getSparkDataType(dataType: String): DataType = {
    dynamoDBSparkMapping(dataType)
  }
}




class DefaultSource extends DataSourceV2 with ReadSupport {
  def createReader(options: DataSourceOptions): DynamoDBReader = {
    new DynamoDBReader(options)
  }
}

class DynamoDBReader(options: DataSourceOptions) extends DataSourceReader {

  implicit val formats = DefaultFormats
  val dynamoTableName: String = options.get("tableName").get
  val awsRegion: String = options.get("awsRegion").get
  val numOfSegments: Int = options.get("segmentsNo").get.toInt
  val schemaJson: String = options.get("schema").get
  val scanItemLimit: Int = options.get("itemLimit").get.toInt
  var sparkConvertedSchema = readSchema()

  def readSchema(): StructType = {
    implicit val formats = DefaultFormats
    val jsonObj = parse(schemaJson)
    val schema = jsonObj.extract[Schema]
    StructType(
      schema.attributes.map(
        record =>
          StructField(
            record.attributeName,
            SchemaConverter.getSparkDataType(record.attributeSubDataType),
            true
          )
      )
    )
  }

  def planInputPartitions = {
    val sparkContext = SparkSession.builder.getOrCreate().sparkContext
    val factoryList = new java.util.ArrayList[InputPartition[InternalRow]]
    val sparkConvertedSchema = readSchema
    // Segment planning get from user
    val segments = 0 until numOfSegments
    for (i: Int <- segments){
      val segmentDetails = SegmentDetails(
        tableName = dynamoTableName ,
        region = awsRegion,
        itemLimit = scanItemLimit,
        totalSegments = numOfSegments,
        segment = i
      )
      factoryList.add(
        new DynamoDBSegment(schemaJson, sparkConvertedSchema, segmentDetails)
      )
    }
    factoryList
  }

}
class DynamoDBSegment(
                       schema: String,
                       sparkSchema: StructType,
                       segmentDetails: SegmentDetails
                     ) extends InputPartition[InternalRow] {

  def createPartitionReader = new DynamoSegmentReader(
    schema,
    sparkSchema,
    segmentDetails
  )
}

class DynamoSegmentReader(
                           schema: String,
                           sparkSchema: StructType,
                           segmentDetails: SegmentDetails
                         )   extends InputPartitionReader[InternalRow] {

  private var partitionSegmentDetails = segmentDetails
  private var tableName: String = partitionSegmentDetails.tableName
  private var scanItemLimit = partitionSegmentDetails.itemLimit
  private var segment = partitionSegmentDetails.segment
  private var totalSegments = partitionSegmentDetails.totalSegments
  private val client: AmazonDynamoDB = AmazonDynamoDBClientBuilder.standard().build()
  private var exclusiveStartKey: Map[String, AttributeValue] = null
  private var itemList: ArrayList[java.util.Map[String,com.amazonaws.services.dynamodbv2.model.AttributeValue]] = new ArrayList[java.util.Map[String,com.amazonaws.services.dynamodbv2.model.AttributeValue]]()
  private var itemListCounter:Int = 0
  private var totalScannedItemCount: Int = 0
  private var totalScanRequestCount: Int = 0
  private var itemSparkSchema = sparkSchema

  def next() = {
    // Segment fetch
    if ( itemListCounter == totalScannedItemCount - 1 || exclusiveStartKey == null ) {
      var scanRequest: ScanRequest = new ScanRequest()
        .withTableName(tableName)
        .withLimit(scanItemLimit)
        .withExclusiveStartKey(exclusiveStartKey)
        .withTotalSegments(totalSegments)
        .withSegment(segment)
      // Put a try catch later
      var result: ScanResult = client.scan(scanRequest)
      itemList.addAll(result.getItems())
      totalScannedItemCount = totalScannedItemCount + result.getCount()
      totalScanRequestCount = totalScanRequestCount + 1
      exclusiveStartKey = result.getLastEvaluatedKey
    }
    // Return single element
    if ( itemListCounter <= totalScannedItemCount - 1 && exclusiveStartKey != null  )
      true
    else
      false
  }

  def get() = {
    var dynamoItem = itemList(itemListCounter)
    itemListCounter = itemListCounter + 1
    var item = ItemUtils.toItem(dynamoItem)
    val values = itemSparkSchema.map {
      attribute => {
        attribute.dataType match {
          case DoubleType => if (item.isPresent(attribute.name) && item.isNull(attribute.name) == false ) UTF8String.fromString(item.get(attribute.name).toString) else null
          case StringType => if (item.isPresent(attribute.name) && item.isNull(attribute.name) == false ) UTF8String.fromString(item.get(attribute.name).toString) else null
          case BinaryType => if (item.isPresent(attribute.name) && item.isNull(attribute.name) == false ) item.getBinary(attribute.name) else null
          case BooleanType => if (item.isPresent(attribute.name) && item.isNull(attribute.name) == false ) item.getBOOL(attribute.name) else null
        }
      }
    }
    InternalRow.fromSeq(values)
  }
  def close() = {
    // Don't know what to do
    //conn.close()
  }
}