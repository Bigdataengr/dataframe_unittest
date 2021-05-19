import DataParser.extractBirthDate
import org.scalatest.funsuite.AnyFunSuite

class DataParserTest extends AnyFunSuite with SparkSessionTestWrapper with DataFrameTestUtils {

  import spark.implicits._

  test("DataFrame Schema Test") {

    val sourceDf = Seq(
      ("Adore", "Shaddock", "19981217-73959"),
      ("Lorenza", "Kiersten", "19621220-14807"),
      ("Mureil", "Willie", "19781211-72222")
    ).toDF("name", "surname", "identification_number")

    val resDf = extractBirthDate(sourceDf)

    val expectedDf = Seq(
      ("Adore", "Shaddock", "19981217-73959", "19981217", "1998", "12", "17"),
      ("Lorenza", "Kiersten", "19621220-14807", "19621220", "1962", "12", "20"),
      ("Mureil", "Willie", "19781211-72222", "19781211", "1978", "12", "11")
    ).toDF("name", "surname", "identification_number", "birth_date", "year", "month", "day")

    assert(assertSchema(resDf.schema, expectedDf.schema))
  }

  test("DataFrame Data Test") {
    val sourceDf = Seq(
      ("Jackie", "Ax", "19861126-29967"),
      ("Vanessa", "Campball", "19881021-86591"),
      ("Willetta", "Reneta", "19991125-38555")
    ).toDF("name", "surname", "identification_number")

    val resDf = extractBirthDate(sourceDf)

    val expectedDf = Seq(
      ("Jackie", "Ax", "19861126-29967", "19861126", "1986", "11", "26"),
      ("Vanessa", "Campball", "19881021-86591", "19881021", "1988", "10", "21"),
      ("Willetta", "Reneta", "19991125-38555", "19991125", "1999", "11", "25")
    ).toDF("name", "surname", "identification_number", "birth_date", "year", "month", "day")

    assert(assertData(resDf, expectedDf))
  }
}