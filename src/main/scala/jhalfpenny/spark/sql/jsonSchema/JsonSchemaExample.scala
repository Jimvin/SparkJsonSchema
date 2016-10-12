package jhalfpenny.spark.sql.jsonSchema

import jhalfpenny.spark.sql.jsonSchema.util.SchemaUtil
import org.apache.spark.sql.types.StructType
import com.quartethealth.spark.fixedwidth.FixedwidthContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import SchemaUtil._

/**
  * Created by jhalfpenny on 11/10/2016.
  */
object JsonSchemaExample {

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setAppName("Spark Count"));
    val sqlContext = new SQLContext(sc) // sc is defined in the spark console

    // Fixed-width data file
//    val dataFile = "file:///home/cloudera/fixed-width/data/datafile.txt"
    val dataFile = args(0)

    // JSON file containing column name, type and field width. One entry per line per column
    // e.g. {"columnName": "id", "columnType": "IntegerType", "columnWidth": "5"}
//    val jsonSchema = "file:///home/cloudera/fixed-width/data/new_schema.json"
    val jsonSchema = args(1)

    // Load JSON schema from file
    val jsonString = sc.wholeTextFiles(jsonSchema).first._2

    // Read the column definitions from the JSON string
    val columns: List[Map[String,Any]] = getColumns(jsonString)

    // Convert the json schema definition to a StructType schema object
//    val tableSchema = StructType(ParseJsonSchema(schemaDefinition))
    val tableSchema = StructType(createSparkDFSchema(columns))

    // create an array of the column widths from the json schema definition
    val columnWidths: Array[Int] = getColumnWidths(columns)

    // We can store other attributes like the table name in the JSON schema definition
    val tableName = getTableName(jsonString)

    // Use the schema object to build an DataFrame on the fixed-width data file
    val result = sqlContext.fixedFile(
        dataFile,
        columnWidths,
        tableSchema,
        useHeader = false
      )

    result.write.parquet("/user/cloudera/data/" + tableName)
  }
}
