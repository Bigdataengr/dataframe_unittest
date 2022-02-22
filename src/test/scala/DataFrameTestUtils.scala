import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait DataFrameTestUtils {

  def assertSchema(schema1: StructType, schema2: StructType, checkNullable: Boolean = true): Boolean = {
    val s1 = schema1.fields.map(f => (f.name, f.dataType, f.nullable))
    val s2 = schema2.fields.map(f => (f.name, f.dataType, f.nullable))
    if (checkNullable) {
      s1.diff(s2).isEmpty
    }
    else {
      s1.map(s => (s._1, s._2)).diff(s2.map(s => (s._1, s._2))).isEmpty
    }
  }

  def assertData(df1: DataFrame, df2: DataFrame): Boolean = {
    if (df1.head(1).isEmpty & df2.head(1).nonEmpty) {
      false
    } else {
      val data1 = df1.collect()
      val data2 = df2.collect()
      data1.diff(data2).isEmpty
    }
  }
}

