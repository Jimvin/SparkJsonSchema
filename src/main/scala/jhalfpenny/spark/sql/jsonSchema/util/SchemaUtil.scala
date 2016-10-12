package jhalfpenny.spark.sql.jsonSchema.util

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

import scala.util.parsing.json.JSON

/**
  * Created by jhalfpenny on 11/10/2016.
  */
object SchemaUtil {

  def resolveColumnType(columnType: String) = columnType match {
    case "Byte" => ByteType
    case "Short" => ShortType
    case "Integer" => IntegerType
    case "Long" => LongType
    case "Float" => FloatType
    case "Double" => DoubleType
    case "String" => StringType
    case "Binary" => BinaryType
    case "Boolean" => BooleanType
    case "Timestamp" => TimestampType
    case _ => StringType
  }

  def parseColumn(columnName: String, columnType: String): StructField = {
    StructField(columnName, resolveColumnType(columnType))
  }

  def parseJsonSchema(schemaDefinition: DataFrame): StructType = {
      StructType(schemaDefinition.collect.map(column => parseColumn(column(0).toString(), column(1).toString()))
    )
  }

  def createSparkDFSchema(columns: List[Map[String,Any]]): StructType = {
    StructType(columns.map(column => parseColumn(column("columnName").asInstanceOf[String],
      column("columnType").asInstanceOf[String])))
  }

  def getMapValueFromJson(jsonString: String): List[Map[String,String]] = {
    JSON.parseFull(jsonString).get.asInstanceOf[Map[String,List[Map[String,String]]]]("columns")
  }

  def getStringValueFromJson(jsonString: String, key: String): String = {
    JSON.parseFull(jsonString).get.asInstanceOf[Map[String,String]](key)
  }

  def getIntegerValueFromJson(jsonString: String, key: String): Integer = {
    JSON.parseFull(jsonString).get.asInstanceOf[Map[String,Integer]](key)
  }

  def getColumns(jsonString: String): List[Map[String,String]] = {
    JSON.parseFull(jsonString).get.asInstanceOf[Map[String,List[Map[String,String]]]]("columns")
  }

  def getTableName(jsonString: String): String = {
    getStringValueFromJson(jsonString, "tableName")
  }

  def getColumnWidths(columns: List[Map[String,Any]]): Array[Int] = {
    columns.map(column => column("columnWidth").toString  .toInt).toArray
  }
}
