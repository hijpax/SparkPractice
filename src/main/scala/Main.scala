import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Timestamp

object Main extends App {

  val spark = SparkSession.builder()
    .appName("Main")
    .config("spark.master","local[*]")
    .getOrCreate()


  def readDF(filename:String) = spark.read
    .option("inferSchema",true)
    .option("header","true")
    .csv(s"src/main/resources/data/$filename")

  def writeDS[T](origin:Dataset[T],filename:String) = origin.write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header","true")
    .save(s"src/main/resources/data/$filename")


  val eventsOctDF = readDF("2019-october-02")

  val eventsFebDF = readDF("2020-february-02")

  val eventsDF = eventsOctDF.union(eventsFebDF)


  import spark.implicits._

  case class Event(
                    user_session:String,
                    user_id:Long,
                    date_time:Timestamp,
                    event_type:String,
                    product_id:Long,
                    price:Double
                  )

  case class Product(
                      id:Long,
                      category_id:Long,
                      category_code:Option[String],
                      brand:Option[String]
                    )


  val eventsDS = eventsDF
    .withColumn("date_time",to_timestamp(col("event_time"),"yyyy-MM-dd HH:mm:ss z"))
    .select("event_type","date_time","user_id","user_session","product_id","price")
    .as[Event]

  val productsDS = eventsDF
    .selectExpr("product_id as id","category_id","category_code","brand")
    .distinct()
    .as[Product]


  writeDS(eventsDS,"events")

  writeDS(productsDS,"products")

  eventsDS.joinWith(productsDS,eventsDS.col("product_id") === productsDS.col("id"))

}
