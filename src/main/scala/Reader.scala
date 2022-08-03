import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types._

object Reader {
  val spark = SparkSession.builder()
    .config("spark.master","local[8]")
    .appName("Reader Dataframes")
    .getOrCreate()


  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
  def fileExists(filename:String) = fs.exists(new Path(s"C:/data/$filename"))


  //Prepare Schema
  val eventSchema = StructType(Array(
    StructField("event_time",TimestampType),
    StructField("event_type",StringType),
    StructField("product_id",LongType),
    StructField("category_id",LongType),
    StructField("category_code",StringType),
    StructField("brand",StringType),
    StructField("price",DoubleType),
    StructField("user_id",LongType),
    StructField("user_session",StringType),
  ))

  def readDF(filename:String) =
      spark.read
        .schema(eventSchema)
        .option("dateFormat", "YYYY-MM-dd HH:mm:ss z")
        .option("header", "true")
        .csv(s"C:/data/$filename")
}
