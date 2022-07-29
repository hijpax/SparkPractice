import org.apache.spark.sql.SparkSession

object Main extends App {
  val spark = SparkSession.builder()
    .config("spark.master","local[*]")
    .getOrCreate()

}
