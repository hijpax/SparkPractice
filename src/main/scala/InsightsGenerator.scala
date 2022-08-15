import Reader.readDF
import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * This singleton object contains the main method to generate the reports, show results in the console and save in csv format.
 * Performs the process from a clean sample of the data in parquet format.
 */
object InsightsGenerator {

  /**
   *It shows the results in the console of the specified Datframe, and saves the results in csv format with the indicated path and name.
   * The rows shown depend on the 'show' parameter which defaults to 20.
   *
   * @param df Datagram to save and show in console
   * @param path Path of the destination directory in which the resulting csv file will be saved
   * @param insightName Name of the report to display in the console and assigned to the resulting file
   * @param show number of rows to show in console
   */
  def saveResult(df:DataFrame,path:String,insightName:String,show:Integer=20): Unit = {
    val destinationPath = s"$path/results/$insightName"

    println(s"\n\t\t<----- Report: $insightName ----->")

    println("Showing in console...")
    df.show(show)

    println(s"Saving file in: $destinationPath...")
    df
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(destinationPath)
  }

  /**
   * Formats a numeric column to present a currency format (dollar).
   * @return A string from a numeric column formatted as currency
   */
  val currencyFormat: Column => Column = (number:Column) => concat(lit("$ "),format_number(number,2))

  /**
   * Formats a column to present an amount as a percentage with two digits of precision.
   * @return A string representing a percentage.
   */
  val percentageFormat: Column => Column = (number:Column) => concat(format_number(number*100,2),lit(" %"))

  /**
   * Read the parquet files within the specified path, to generate the 7 reports on sales, statistics and user interactions.
   * Shows results in the console and saves them in csv format in the same source directory.
   * @param path Path of the directory that contains the parquet files to read.
   * @return Success message at the end of the process.
   */
  def generateInsights(path: String): String = {

    val events2019DF = readDF(path, "parquet","*") //Read all files of the dataset
    val totalRows = events2019DF.count()

    println(s"\nTotal rows to process: $totalRows")

    val wCategory = Window.partitionBy("category_id")
    val wProduct = Window.partitionBy("product_id","category_id")

    // What are the best-selling product categories (according to the number of events "purchase")?

    //Get only the purchase events, each row with the column that contains the total number of events grouped by product and category.
    val purchaseEventsDF = events2019DF
      .where(col("event_type")==="purchase")
      .withColumn(
        "count_purchase_product", count(lit(1)).over(wProduct)
      )
      .withColumn(
        "sales_product", sum(col("price")).over(wProduct)
      )

    // Filter only the best product of each category according to the number of purchase events, eliminating duplicates
    var df = purchaseEventsDF
      .withColumn(
        "count_purchase_category", count(lit(1)).over(wCategory)
      )
      .withColumn(
        "sales_category", sum(col("price")).over(wCategory)
      )
      .withColumn(
        "rank",dense_rank().over(wCategory.orderBy(col("count_purchase_product").desc_nulls_last))
      )
      .where(col("rank") === 1)
      .select("category_id","category_code","count_purchase_category","sales_category","product_id","count_purchase_product","sales_product")
      .distinct()

      //Sort by number of purchases per category, get only the best 10 categories and format the result
      .orderBy(col("count_purchase_category").desc_nulls_last)
      .limit(10)
      .select(
        col("category_id"),
        col("category_code"),
        format_number(col("count_purchase_category"),0).as("count_purchase_category"),
        currencyFormat(col("sales_category")).as("sales_category"),
        col("product_id").as("best_selling_product_id"),
        format_number(col("count_purchase_product"),0).as("count_purchase_product"),
        currencyFormat(col("sales_product")).as("sales_product"),
        percentageFormat(col("sales_product")/col("sales_category")).as("category_sales_per")
      )

    saveResult(df, path,"the-best-selling-product-categories")

    // What are the hours with the highest number of interactions?
    //Extract the time from the 'event_time' column.
    // Group all records according to time and, through the pivot function, get the count according to each of the three event types
    df = events2019DF
      .withColumn("hour",hour(col("event_time")))
      .groupBy("hour")
      .pivot("event_type")
      .agg(count(lit(1)))

      //To the result, add the column with the total interactions and use it to obtain the percentage of each type of event
      .withColumn("total",col("view")+col("purchase")+col("cart"))
      .withColumn("view_per",percentageFormat(col("view")/col("total")))
      .withColumn("purchase_per",percentageFormat(col("purchase")/col("total")))
      .withColumn("cart_per",percentageFormat(col("cart")/col("total")))

      //Format the result and sort by total interactions
      .select(
        col("hour"),
        format_number(col("view"),0).as("total_views"),
        col("view_per"),
        format_number(col("cart"),0).as("total_cart"),
        col("cart_per"),
        format_number(col("purchase"),0).as("total_purchase"),
        col("purchase_per"),
        format_number(col("total"),0).as("total_interactions")
      )
      .orderBy(col("total").desc_nulls_last)


    saveResult(df, path,"the-hours-with-the-highest-number-of-interactions",24)

    // Products with the highest purchase recurrence per month
    val wMonth = Window.partitionBy("month")

    //Extract the month from the 'event_time' column, select only the columns to identify the product, the brand, the month, the user and the user session.
    //Filter only 'purchase' events and remove duplicates
    df = events2019DF
      .withColumn("month",date_format(col("event_time"),"MMMM"))
      .select("product_id","brand", "month", "user_id","user_session")
      .where(col("event_type") === "purchase")
      .distinct()

      //Group the result in order to obtain the count of sessions in which the same user bought each product. (recurrence)
      .groupBy("product_id","brand", "month", "user_id") //how many times user_id has bought the same product in different sessions per month
      .agg(count(lit(1)).as("recurrence"))
      .where(col("recurrence") > 1)

      //Add a temporary column, to label each recurrence with its interval.
      .withColumn(
        "times",
        when(col("recurrence") > 1 && col("recurrence") < 16,"2-15")
          .when(col("recurrence") >= 16 && col("recurrence") < 31,"16-30")
          .otherwise(">30")
      )

      //Group according to product and month, obtain the recurrence count divided by intervals thanks to the pivot function
      .groupBy("product_id","brand","month")
      .pivot("times")
      .count()
      .na.fill(0) //anadir ceros en lugar de nullos

      //add the total recurrences and a rack to sort in descending order in each month
      .withColumn(
        "total_recurrence",
        col(">30")+col("16-30")+col("2-15")
      )
      .withColumn(
        "rank",
        dense_rank().over(wMonth.orderBy(col("total_recurrence").desc))
      )

      //Format the result and get only the 10 highest recurrences of each month
      .select(
        col("rank").as("No"),
        col("month"),
        col("product_id"),
        col("brand"),
        col("2-15").as("2-15 times"),
        col("16-30").as("16-3 times"),
        col(">30").as(">30 times"),
        col("total_recurrence")
      )
      .where(col("rank")<=10)
      .orderBy(col("month").desc,col("rank"))


    saveResult(df, path, "products-with-the-highest-purchase-recurrence-per-month",30)

    // How do sales of the most selling products evolve each month?
    //From the dataframe used in report 1 (the best-selling product categories) extract the month from the 'event_time' column and add the total sales per month.
    //Group your sales by product and brand to obtain the total sales of each month and the percentage of the monthly total (in columns through the pivot function)
    df = purchaseEventsDF
      .withColumn("month",date_format(col("event_time"),"MMM"))
      .withColumn("total_sales_month",sum("price").over(wMonth))
      .groupBy("product_id","sales_product","brand")
      .pivot("month")
      .agg(
        currencyFormat(sum(col("price"))).as("total"),
        percentageFormat(sum(col("price")/col("total_sales_month"))).as("percentage")
      )

      //Format the result, order by total product sales and limit to the 20 best selling products.
      .select(
        col("product_id"),
        col("brand"),
        col("Oct_total"),
        col("Oct_percentage"),
        col("Nov_total"),
        col("Nov_percentage"),
        col("Dec_total"),
        col("Dec_percentage"),
        currencyFormat(col("sales_product")).as("total_sales_product")
      )
      .orderBy(col("sales_product").desc)
      .na.fill("0") // Replace nulls with zeros
      .limit(20)

    saveResult(df, path, "sales-of-the-most-selling-products-evolve-each-month")


    // What is the date with the most sales revenue? And how much does it vary from the average daily sales?

    //Filter only the 'purchase' events, extract the date from the 'event_time' column and group according to the latter to obtain a dataframe that indicates the total daily sales
    val salesPerDateDF = events2019DF
      .where(col("event_type")==="purchase")
      .withColumn("date",date_format(col("event_time"),"yyyy-MM-dd"))
      .groupBy("date")
      .agg(
        sum("price").as("sales")
      )

    //Generate a summary table with measures of variability (range, standard deviation, coefficient of variation) of daily sales
    val salesVariationDF = salesPerDateDF
      .agg(
        max("sales").as("max"),
        min("sales").as("min"),
        avg("sales").as("avg"),
        stddev("sales").as("stddev")
      )
      .withColumn("range",col("max") - col("min"))
      .withColumn("coefficient_var",col("stddev")/col("avg"))

      // a- Sales variability measures per date
    //Format the result of the summary table on the variability of daily sales
    df = salesVariationDF
      .select(
        currencyFormat(col("min")).as("min_sales"),
        currencyFormat(col("max")).as("max_sales"),
        currencyFormat(col("range")).as("range_sales"),
        currencyFormat(col("avg")).as("avg_sales"),
        currencyFormat(col("stddev")).as("stddev"),
        percentageFormat(col("coefficient_var")).as("coefficient_var")
      )

    saveResult(df, path, "sales-variability-measures-per-date")

    // From the table above, get the average daily sales
    val salesAVG = salesVariationDF.select("avg").head().getDecimal(0)

      // b- Variation of the dates with the most sales over the average sales

    //Add a column with the amount that each daily sale deviates from the average along with the percentage that this deviation represents with respect to said average
    //Sort by daily sales and limit only to the best 20 days.
    df = salesPerDateDF
      .withColumn(
        "deviation",
        currencyFormat(
          col("sales")-salesAVG
        )
      )
      .withColumn(
        "deviation_per",
        percentageFormat(
          (col("sales")-salesAVG)/salesAVG
        )
      )
      .orderBy(col("sales").desc)
      .limit(20)

    saveResult(df, path, "variation-of-the-dates-with-the-most-sales-over-the-average-sales")

    // Interactions avg according to days of the week (Monday - Sunday)
    //Add a temporary column with the value of 1, extract the day of week from the 'event_time' column
    df = events2019DF
      .groupBy(col("event_time"),col("event_type"))
      .agg(count("event_time").as("temp_count"))
      .withColumn("day", date_format(col("event_time"), "EEEE"))

      //Group by date and use the temporary column to calculate the total of events according to the day of the week and its average
      // classify the count and average according to the type of event through the pivot function
      .groupBy(col("day"))
      .pivot("event_type")
      .agg(
        round(avg("temp_count"),2).as("avg"),
        count("temp_count").as("count")
      )

      // Sort the resulting columns, add the total of interactions and order according to the count of 'purchase' events
      .select("day","purchase_count","purchase_avg","cart_count","cart_avg","view_count","view_avg")
      .withColumn("total_interactions",col("purchase_count")+col("view_count")+col("cart_count"))
      .orderBy(col("purchase_count").desc_nulls_last)


    saveResult(df, path, "interactions-avg-according-to-days-of-the-week")

    //Price variation of the best-selling products
    //Select only the product identifier, mark the total sales per product and its price, then remove duplicates
    df = purchaseEventsDF
      .select("product_id","brand","sales_product","price")
      .distinct()

      //Group by product to obtain price variability indicators for each product
      .groupBy("product_id","brand","sales_product")
      .agg(
        max("price").as("max"),
        min("price").as("min"),
        avg("price").as("avg"),
        stddev("price").as("stddev")
      )
      .withColumn("range",col("max") - col("min"))
      .withColumn("coefficient_var",col("stddev")/col("avg"))

      //Format the results, order according to products with the highest total sales and limit to the first 20
      .select(
        col("product_id"),
        col("brand"),
        currencyFormat(col("sales_product")).as("total_sales"),
        currencyFormat(col("avg")).as("avg"),
        currencyFormat(col("range")).as("range"),
        currencyFormat(col("stddev")).as("stddev"),
        percentageFormat(col("coefficient_var")).as("coefficient_var")
      )
      .orderBy(col("sales_product").desc)
      .limit(20)

    saveResult(df, path, "price-variation-of-the-best-selling-products")

    // Products added to cart the most but not purchased (per month)

    // What is the number of new users per month and with which product did they have the most interaction?

    // What is the range of duration of the sessions in which a purchase is made? (compared to visit-only sessions)
    /*val wSession = Window.partitionBy("user_session")
    val sessionsWithDuration = events2019DF
      .withColumn("start_session_time",min("event_time").over(wSession))
      .withColumn("finish_session_time",max("event_time").over(wSession))
      .withColumn("session_duration_seconds",col("finish_session_time").cast(LongType) - col("start_session_time").cast(LongType))
      .select("user_session","session_duration_seconds","event_type")
      .distinct()

    val purchaseSessions = sessionsWithDuration
      .where(col("event_type") === "purchase" )
      .withColumn("session_type",lit("purchase"))
      .select("user_session","session_duration_seconds","session_type")
      .distinct()

    val onlyCartSessions = sessionsWithDuration
      .join(purchaseSessions,sessionsWithDuration.col("user_session") === purchaseSessions.col("user_session"),"left_anti")
      .where(col("event_type") === "cart")
      .withColumn("session_type",lit("cart_view_only"))
      .select("user_session","session_duration_seconds","session_type")
      .distinct()

    val onlyViewSessions = sessionsWithDuration
      .join(onlyCartSessions,onlyCartSessions.col("user_session") === sessionsWithDuration.col("user_session"),"left_anti")
      .withColumn("session_type",lit("view_only"))
      .select("user_session","session_duration_seconds","session_type")
      .distinct()

    df = purchaseSessions
      .union(onlyCartSessions)
      .union(onlyViewSessions)
      .na.drop()

    val maxDuration = df.agg(max("session_duration_seconds")).head().getLong(0)
    val interval = maxDuration/10

    df.withColumn("range", col("session_duration_seconds") - (col("session_duration_seconds") % interval))
      .groupBy("range")
      .pivot("session_type")
      .count()
      .na.fill(0)
      .withColumn("ranges", concat(round(col("range")/60), lit(" - "), round((col("range") + interval)/60),lit(" minutes"))) //optional one
      .select("ranges","purchase","cart_view_only","view_only")
      .orderBy("range")*/
    // .show(false)


    "\n\nReports save successfully."
  }

}
