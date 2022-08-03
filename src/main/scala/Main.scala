import Reader.readDF
import org.apache.spark.sql.functions._

object Main extends App {
  val octDF = readDF("2019-Oct.csv") //Dataset of events in October 2019
  val novDF = readDF("2019-Nov.csv") //Dataset of events in November 2019
  val decDF = readDF("2019-Dec.csv") //Dataset of events in December 2019

  //Union all data
  val events2019DF = octDF.union(novDF)

  // 10 best selling products
  events2019DF
    .where(col("event_type") === "purchase" )
    .groupBy("product_id","brand")
    .agg(count("*").as("sales"))
    .orderBy(col("sales").desc_nulls_last)
    .limit(10)

  // 10 most viewed products with their sales
  events2019DF
    .where(col("event_type") === "view" )
    .groupBy("product_id","brand")
    .agg(count("*").as("views"))
    .orderBy(col("views").desc_nulls_last)
    .limit(10)

  // 10 days with the most interactions (events)
  events2019DF
    .withColumn("event_date",to_date(col("event_time")))
    .groupBy("event_date")
    .agg(count("*").as("interactions"))
    .orderBy(col("interactions").desc_nulls_last)
    .limit(10)

  // 5 best-selling product categories
  events2019DF
    .where(col("event_type") === "purchase")
    .groupBy("category_id","category_code")
    .agg(count("*").as("sales"))
    .orderBy(col("sales").desc_nulls_last)
    .limit(5)

  // 5 brands with more interaction
  events2019DF
    .groupBy("brand")
    .agg(count("*").as("interactions"))
    .orderBy(col("interactions").desc_nulls_last)
    .limit(5)

  // Interactions avg according to days of the week (Monday - Sunday)
  events2019DF
    .groupBy(col("event_time"))
    .agg(count("event_time").as("temp_count"))
    .withColumn("day",date_format(col("event_time"),"EEEE"))
    .groupBy(col("day"))
    .agg(
      count("*").as("interactions"),
      avg("temp_count").as("avg_interactions")
    )
    .orderBy(col("interactions").desc_nulls_last)
    .show()

}
