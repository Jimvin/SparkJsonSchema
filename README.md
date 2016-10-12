




## Building
This example uses the spark-fixedwidth library from Quartet Health, which is based on databricks-spark-csv:
https://github.com/quartethealth/spark-fixedwidth

For this example I built the library and then created a local Maven repo, which you can create using the command below. Make sure to update your pom.xml to include the path to this repo before building.
 
```
mvn install:install-file -DlocalRepositoryPath=repo  -DcreateChecksum=true -Dpackaging=jar -Dfile=spark-fixedwidth-assembly-1.0.jar -DgroupId=com.quartethealth -DartifactId=spark-fixedwidth -Dversion=1.0
```

With this done and your pom.xml update you can run `mvn package` to build the example JAR file.


## Running
An example class `JsonSchemaExample` is included which demonstrates how to convert a fixed-width field text file into Parquet. The job requires 3 parameters:
- The location of the data file
- The location of the JSON schema definition
- The location to write the resulting Parquet files to

The JSON schema definition contains metadata describing the table, including the name, type and width of the fixed-width columns. 

```json
{
  "columns": [
    {"columnName": "id", "columnType": "Integer", "columnWidth": "5"},
    {"columnName": "firstName", "columnType": "String", "columnWidth": "10"},
    {"columnName": "lastName", "columnType": "String", "columnWidth": "10"},
    {"columnName": "gender", "columnType": "String", "columnWidth": "1"},
    {"columnName": "dateOfBirth", "columnType": "String", "columnWidth": "10"}
  ],

  "tableName": "people",
  "comment": "This is a test fixed-width field table",
  "partition": "partition_spec_goes_here"
}
```
To run the example submit the job using the command below. A new directory named /user/cloudera/data/people (i.e. output directory + table name) will be created containing the parquet files.
```
spark-submit --master yarn --jars spark-fixedwidth-assembly-1.0.jar \
  --class jhalfpenny.spark.sql.jsonSchema.JsonSchemaExample \
  JsonSchemaTest-0.0.1-SNAPSHOT.jar \
  /user/cloudera/fixed-width/data/datafile.txt \
  /user/cloudera/fixed-width/data/schema.json \
  /user/cloudera/data
```
We can use spark-shell  to examine the Parquet data files to show that the schema and data are correct.
```scala
scala> sqlContext.read.parquet("/user/cloudera/data/people").printSchema
root
 |-- id: integer (nullable = true)
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- dateOfBirth: string (nullable = true)
 
scala> sqlContext.read.parquet("/user/cloudera/data/people").show
+---+---------+---------+------+-----------+
| id|firstName| lastName|gender|dateOfBirth|
+---+---------+---------+------+-----------+
|  7|    Nancy|  Bentley|     F| 1989-17-12|
|  8|      Ben|    Smoak|     M| 1983-09-18|
|  9|    Simon| Lansdown|     M| 1985-06-07|
| 10|     Fred|     West|     M| 1992-04-23|
|  1|     John|    Smith|     M| 1970-07-01|
|  2|     Adam|    Jones|     M| 1963-12-11|
|  3|    Bruce|    Davis|     M| 1981-01-17|
|  4|   Sheila|   Fraser|     F| 1984-08-03|
|  5|      Tom|    Berne|     M| 1974-17-12|
|  6| Gertrude|Shoemaker|     F| 1991-03-09|
+---+---------+---------+------+-----------+
```
