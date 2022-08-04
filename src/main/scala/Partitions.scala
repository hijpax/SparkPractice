import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, lit, to_date}
import Reader.readDF

object Partitions extends App {

  //October 2019
  val eventsDF = readDF("2019-*.csv")

  eventsDF.sample(0.1)
    .write
    .format("csv")
    .mode(SaveMode.Overwrite)
    .option("header","true")
    .save("C:/data/samples")
}
