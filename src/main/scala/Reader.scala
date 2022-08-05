import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._
import org.apache.log4j.{Level, Logger}

import scala.util.Try

object Reader {

  val spark: SparkSession = SparkSession.builder()
    .config("spark.master","local[*]")
    .appName("Spark Project")
    .getOrCreate()

  val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  def fileExists(filePath:String): Try[Boolean] = Try(fs.exists(new Path(filePath)))

  // Prepare the default Schema,
  // Based on information about null fields, obtained from https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store
  // and a EDA report generated from a sample of the dataset
  val defaultEventSchema: StructType = StructType(Array(
    StructField("event_time",TimestampType,nullable = false),
    StructField("event_type",StringType,nullable = false),
    StructField("product_id",LongType,nullable = false),
    StructField("category_id",LongType,nullable = false),
    StructField("category_code",StringType,nullable = true),
    StructField("brand",StringType,nullable = true),
    StructField("price",DoubleType,nullable = false),
    StructField("user_id",LongType,nullable = false),
    StructField("user_session",StringType,nullable = false),
  ))

 // Read the file in format csv from the provided
 // if is not specified the schema, the default is used
  def readDF(path:String,filename:String,schema:StructType=defaultEventSchema): DataFrame =
      spark.read
        .schema(schema)
        .option("dateFormat", "YYYY-MM-dd HH:mm:ss z")
        .option("header", "true")
        .csv(s"$path/$filename")

  // Obtain a sample
  def generateSample(sourcePath:String,originFileName:String="2019-*.csv",fraction:Double = 0.1):String = {

    println(s"generating a sample size of ${fraction*100}% of the data...")

    val destinationPath = s"$sourcePath/sample"

    // Read the complete dataset from 3 files correspond to october, november and december at 2019
    val eventsDF = readDF(sourcePath,originFileName)

    //Get a 10% sample to optimize performance insights analysis on my PC
    eventsDF.sample(fraction)
      .write
      .format("csv")
      .mode(SaveMode.Ignore)
      .option("header","true")
      .save(destinationPath)

    //Return the path of the sample
    destinationPath
  }

}
