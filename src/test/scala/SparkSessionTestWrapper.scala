import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[1]")
      .appName("Local Test")
      .getOrCreate()
}
