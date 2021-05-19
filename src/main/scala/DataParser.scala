import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, regexp_extract, substring}

object DataParser extends App {

  def extractBirthDate(inputDf: DataFrame): DataFrame = {
    inputDf
      .withColumn("birth_date", regexp_extract(col("identification_number"), """\d+(?=-)""", 0))
      .withColumn("year", substring(col("birth_date"), 0, 4))
      .withColumn("month", substring(col("birth_date"), 5, 2))
      .withColumn("day", substring(col("birth_date"), 7, 2))
  }
}
