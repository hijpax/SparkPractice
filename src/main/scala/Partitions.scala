import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, to_date}
import Reader.readDF

object Partitions extends App {

  //October 2019
  val octDF = readDF("2019-Oct.csv")

  octDF.select("*")
    .where(to_date(col("event_time"),"yyyy-MM-dd HH:mm:ss z") < to_date(lit("2019-10-02 00:00:00 UTC"),"yyyy-MM-dd HH:mm:ss z"))
    .limit(200)
    .write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/2019-october-02")


  val febDF = readDF("2020-Feb.csv")

  febDF.select("*")
    .where(to_date(col("event_time"),"yyyy-MM-dd HH:mm:ss z") < to_date(lit("2020-02-02 00:00:00 UTC"),"yyyy-MM-dd HH:mm:ss z"))
    .limit(200)
    .write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/2020-february-02")
}
