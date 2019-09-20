package com.goibibo.spark.datasourcev2.dynamoDB.operations

import java.util

import com.goibibo.spark.datasourcev2.dynamoDB.connector.{DynamoConnector, TableConnector, TableIndexConnector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types.{StructType, _}
import org.json4s.DefaultFormats

import scala.collection.JavaConverters._

class DynamoDBReader(options: DataSourceOptions) extends DataSourceReader  {

  implicit val formats: DefaultFormats.type = DefaultFormats

  private val tableName = options.get("tableName").get
  private val indexName = options.get("indexName")
  private val schemaJson = options.get("schema").get
  private val sparkContext = SparkSession.builder.getOrCreate().sparkContext
  private val numPartitions: Int = options.get("readPartitions").map(_.toInt).orElse(sparkContext.defaultParallelism)
  private val  dynamoConnector: DynamoConnector =
    if (indexName.isPresent) new TableIndexConnector(tableName, indexName.get, numPartitions, options)
    else new TableConnector(tableName, numPartitions, options)

  val schema = new StructType()
  def readSchema: StructType = Option(schema).getOrElse(inferSchema())


  def planInputPartitions: util.List[InputPartition[InternalRow]] = {
    makePartitions(numPartitions).asJava
  }

  private def makePartitions(numPartitions: Int): Seq[InputPartition[InternalRow]] = {
    (0 until numPartitions).map(index => new DynamoDBPartition(readSchema, index, dynamoConnector))
  }

  private def inferSchema(): StructType = {
    val inferenceItems =
      if (dynamoConnector.nonEmpty) dynamoConnector.scan(0, Seq.empty, Seq.empty).firstPage().getLowLevelResult.getItems.asScala
      else Seq.empty

    val typeMapping = inferenceItems.foldLeft(Map[String, DataType]())({
      case (map, item) => map ++ item.asMap().asScala.mapValues(inferType)
    })
    val typeSeq = typeMapping.map({ case (name, sparkType) => StructField(name, sparkType) }).toSeq

    if (typeSeq.size > 100) throw new RuntimeException("Schema inference not possible, too many attributes in table.")

    StructType(typeSeq)
  }

  private def inferType(value: Any): DataType = value match {
    case number: java.math.BigDecimal =>
      if (number.scale() == 0) {
        if (number.precision() < 10) IntegerType
        else if (number.precision() < 19) LongType
        else DataTypes.createDecimalType(number.precision(), number.scale())
      }
      else DoubleType
    case list: java.util.ArrayList[_] =>
      if (list.isEmpty) ArrayType(StringType)
      else ArrayType(inferType(list.get(0)))
    case set: java.util.Set[_] =>
      if (set.isEmpty) ArrayType(StringType)
      else ArrayType(inferType(set.iterator().next()))
    case map: java.util.Map[String, _] =>
      val mapFields = (for ((fieldName, fieldValue) <- map.asScala) yield {
        StructField(fieldName, inferType(fieldValue))
      }).toSeq
      StructType(mapFields)
    case _: java.lang.Boolean => BooleanType
    case _: Array[Byte] => BinaryType
    case _ => StringType
  }

}