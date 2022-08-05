import Reader.readDF
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{avg, col, count, date_format, to_date}

object InsightsGenerator {
  def generateInsights(path: String): String = {

    val events2019DF = readDF(path, "*.csv") //Read all file of the dataset

    // 10 best selling products
    var df = events2019DF
      .where(col("event_type") === "purchase")
      .groupBy("product_id", "brand")
      .agg(count("*").as("sales"))
      .orderBy(col("sales").desc_nulls_last)
      .limit(10)

    saveResult(df, path, "10-best-selling-products")

    /** *
     * TODO - add sales of the products in this query
     */
    // 10 most viewed products with their sales
    df = events2019DF
      .where(col("event_type") === "view")
      .groupBy("product_id", "brand")
      .agg(count("*").as("views"))
      .orderBy(col("views").desc_nulls_last)
      .limit(10)

    saveResult(df, path, "10-most-viewed-products-with-their-sales")


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
      .groupBy("brand")
      .agg(count("*").as("interactions"))
      .orderBy(col("interactions").desc_nulls_last)
      .limit(5)

    saveResult(df, path, "5-brands-with-more-interaction")

    // Interactions avg according to days of the week (Monday - Sunday)
    df = events2019DF
      .groupBy(col("event_time"))
      .agg(count("event_time").as("temp_count"))
      .withColumn("day", date_format(col("event_time"), "EEEE"))
      .groupBy(col("day"))
      .agg(
        count("*").as("interactions"),
        avg("temp_count").as("avg_interactions")
      )
      .orderBy(col("interactions").desc_nulls_last)

    saveResult(df, path, "Interactions-avg-according-to-days-of-the-week")

    // Relation views-sales for 10 most viewed products
    // TODO - Relation views-sales for 10 most viewed products query

    // Relation sales-views for 10 most selling products
    // TODO - Relation sales-views for 10 most selling products query

    "\n\nReports save successfully."
  }

  def saveResult(df:DataFrame,path:String,insightName:String): Unit = {
    val destinationPath = s"$path/results/$insightName"

    println(s"\n\t\t<----- Report: $insightName ----->")
    println(s"Saving file in: $destinationPath...")

    df
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Ignore)
      .save(destinationPath)

    println("Showing in console...")
    df.show()
  }
}
