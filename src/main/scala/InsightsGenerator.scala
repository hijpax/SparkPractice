import Reader.readDF
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

object InsightsGenerator {
  def generateInsights(path: String): String = {

    val events2019DF = readDF(path, "parquet","*") //Read all files of the dataset
    val totalRows = events2019DF.count()

    // 10 best selling products
    var df = events2019DF
      .groupBy("product_id", "brand")
      .pivot("event_type")
      .count()
      .withColumn("sales_views_relation",round(col("purchase")/col("view"),3))
      .orderBy(col("purchase").desc_nulls_last)
      .limit(10)

    saveResult(df, path, "10-best-selling-products-with-their-sales-views-relation")


    // 10 days with the most interactions (events)
    df = events2019DF
      .withColumn("event_date", to_date(col("event_time")))
      .groupBy("event_date")
      .agg(count("*").as("interactions"))
      .orderBy(col("interactions").desc_nulls_last)
      .limit(10)

    saveResult(df, path, "10-days-with-the-most-interactions-(events)")

    // 5 best-selling product categories
    df = events2019DF
      .where(col("event_type") === "purchase")
      .groupBy("category_id", "category_code")
      .agg(count("*").as("sales"))
      .orderBy(col("sales").desc_nulls_last)
      .limit(5)

    saveResult(df, path, "5-best-selling-product-categories")

    // 5 brands with more interaction
    df = events2019DF
      .where("brand != 'unknown'")
      .groupBy("brand")
      .agg(count("*").as("interactions"))
      .withColumn("percentage",round(col("interactions")/totalRows*100,2))
      .orderBy(col("interactions").desc_nulls_last)
      .limit(5)

    saveResult(df, path, "5-brands-with-more-interaction")

    // Interactions avg according to days of the week (Monday - Sunday)
    df = events2019DF
      .groupBy(col("event_time"),col("event_type"))
      .agg(count("event_time").as("temp_count"))
      .withColumn("day", date_format(col("event_time"), "EEEE"))
      .groupBy(col("day"))
      .pivot("event_type")
      .agg(
        round(avg("temp_count"),2).as("avg")
      )
      .orderBy(col("purchase").desc_nulls_last)

    saveResult(df, path, "Interactions-avg-according-to-days-of-the-week")

    "\n\nReports save successfully."
  }

  def saveResult(df:DataFrame,path:String,insightName:String): Unit = {
    val destinationPath = s"$path/results/$insightName"

    println(s"\n\t\t<----- Report: $insightName ----->")

    println("Showing in console...")
    df.show()

    println(s"Saving file in: $destinationPath...")
    df
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(destinationPath)
  }
}
