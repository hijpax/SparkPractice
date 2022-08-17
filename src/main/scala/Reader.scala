import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._
import scala.util.Try

/**
 * Object that contains the unique SparkSession of the application and the methods to read data sources and create Dataframes from them, in addition to generating samples and saving them in an easy-to-read format.
 */
object Reader {

  val spark: SparkSession = SparkSession.builder()
    .config("spark.master","local[*]")
    .appName("Spark Project")
    .getOrCreate()

  //Set log Level
  spark.sparkContext.setLogLevel("WARN")

  //To perform patterns in dates
  spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

  //Set the timezone
  spark.conf.set("spark.sql.session.timeZone", "UTC")

  //Set the partitions numbers, result of equation: stage input data/target size
  // 20GB/ 200mb = 100
  spark.conf.set("spark.sql.shuffle.partitions",150)

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
    StructField("price",DecimalType(10,2),nullable = false),
    StructField("user_id",LongType,nullable = false),
    StructField("user_session",StringType,nullable = false),
  ))

 // Read the file in format csv from the provided
 // if is not specified the schema, the default is used

  /**
   * Reads a data source, from it generates a Dataframe through the SparkSession.
   * @param path path to the directory containing the source files.
   * @param format format of the files to read to generate the Df.
   * @param filename Name of the file(s) to read. * for all files in the directory
   * @param schema Schema of the data to be read, if it is omitted, the one defined inside the Reader object is used.
   * @return Dataframe obtained from reading the source files
   */
  def readDF(path:String,format:String="parquet",filename:String,schema:StructType=defaultEventSchema): DataFrame =
      spark.read
        .format(format)
        .schema(schema)
        .option("dateFormat", "YYYY-MM-dd HH:mm:ss z")
        .option("header", "true")
        .load(s"$path/$filename")

  // Obtain a sample

  /**
   * Reads, removes duplicates and handles null values from the specified data source, then generates a data sample depending on the desired fraction and saves it in parquet format in a folder called 'sample' in the same source directory.
   * @param sourcePath Path to the directory containing the source file(s) to read.
   * @param originFileName Name of the file containing the data. * if they are all the files in the directory.
   * @param fraction Fraction of the data to obtain as a sample. Default 0.1 (10%)
   * @return The destination path of the obtained sample files.
   */
  def generateSample(sourcePath:String,originFileName:String="2019-*.csv",fraction:Double = 0.1):String = {

    // Read the complete dataset from 3 files correspond to october, november and december at 2019
    val eventsDF = readDF(sourcePath,"csv",originFileName)

    println(s"\nTotal rows in original dataset: ${eventsDF.count()} rows")
    println(s"\nGenerating a sample with ${fraction*100}% of the data \nand adding default values to null cells in columns 'category_code' and 'brand'...")

    val destinationPath = s"$sourcePath/sample"

    //Get a 10% (default) sample to optimize performance insights analysis on a PC
    eventsDF
      .distinct() //remove duplicate rows
      .na.fill(Map(
        "brand" -> "unknown",
        "category_code" -> "not specified"
      ))
      .sample(fraction)
      .write
      .format("parquet")
      .mode(SaveMode.Overwrite)
      .save(destinationPath)

    //Return the path of the sample
    destinationPath
  }

}
