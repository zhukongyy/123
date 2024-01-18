package com.mi.sql

import org.apache.spark.sql.SparkSession


object area {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MedicalInsurance").master("local").getOrCreate()
    val data = spark.read
      .option("header", "true")
      .csv("src/main/resources/data.csv")

    val areaCheatDF = data
      .select("RES", "地区")
      .toDF()

    areaCheatDF.createTempView("area_cheat")

    // 每个地区医保欺诈占比
    spark.sql(
      """
        SELECT `地区` AS location, ROUND(SUM(RES)/COUNT(*)*100,2) AS rate
        FROM area_cheat
        GROUP BY location
        ORDER BY location
      """)
      .coalesce(1)
      .write
      .format("csv").
      option("header","true")
      .mode("overwrite")
      .save("src/main/resources/deceptionRate")

    spark.stop()
  }
}
