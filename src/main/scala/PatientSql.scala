import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, row_number}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SparkSession, functions => F}


object PatientSql {
  def main(args: Array[String]): Unit = {
    val spark= SparkSession.builder().appName("PatientCount").master("local[32]").getOrCreate()
    val dataDF=spark.read.format("csv")
      .option("header","true")
      .load("src/main/resources/data.csv")
      .withColumn("地区", F.col("地区").cast(IntegerType))

    dataDF.createTempView("patient")
    spark.sql("select `地区` as location, count(*) as sum from patient group by `地区` order by `地区`")
      .coalesce(1)
      .write.format("csv").
      option("header","true")
      .mode("overwrite")
      .save("src/main/resources/patientCount")
    spark.sql("select `地区` as location, round(avg(`就诊次数_SUM`),2) as avg_Allergy from patient group by `地区` order by `地区`")
      .coalesce(1)
      .write.format("csv").
      option("header","true")
      .mode("overwrite")
      .save("src/main/resources/avgAllergy")
    spark.sql("select `地区` as location, round(avg(ALL_SUM),2) as avg_expend from patient group by `地区` order by `地区`")
      .coalesce(1)
      .write.format("csv").
      option("header","true")
      .mode("overwrite")
      .save("src/main/resources/avgExpend")
val windowSpec = Window.partitionBy("地区").orderBy(col("count").desc)

    spark.sql("select `地区`,`医院编码_NN`,count(*) as count from patient group by `地区`,`医院编码_NN` order by`地区`,count(*) desc")
      .withColumn("row_num", row_number().over(windowSpec))
      .where(col("row_num") <= 5)
      .drop("row_num")
        .coalesce(1)
        .write
        .partitionBy("地区")
        .mode("overwrite")
        .format("csv").option("header","true").save("src/main/resources/hospitalTop5")
  }
}
