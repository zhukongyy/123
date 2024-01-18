import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import javax.print.DocFlavor.STRING

object patientRDD {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("PatientCount").setMaster("local")
    val sc = new SparkContext(conf)
    // 读取CSV文件为RDD
    val dataRDD = sc.textFile("src/main/resources/data.csv")

    //patientCount(dataRDD)
    avgAllergy(dataRDD)
    avgExpend(dataRDD)
    sc.stop()

  }
  private def patientCount(dataRDD: RDD[String]): Unit = {
    // 使用map将每一行转换为（地区， 1）的形式
    val locationCountRDD = dataRDD
      .filter(line => !line.startsWith("患者编号")) // 跳过头部
      .map(line => (line.split(",")(82).toInt, 1))

    // 使用reduceByKey将相同地区的计数相加
    val locationSumRDD = locationCountRDD.reduceByKey(_ + _)
    // 将结果按照地区排序
    val sortedLocationSumRDD = locationSumRDD.sortByKey()

    // 将结果保存为CSV文件
    sortedLocationSumRDD
      .map { case (location, sum) => s"$location,$sum" }
      .coalesce(1)
      .saveAsTextFile("src/main/resources/patient_count")

    // 停止SparkContext
  }

  private def avgAllergy(dataRDD: RDD[String]): Unit = {

    dataRDD
      .filter(line => !line.startsWith("患者编号")) // 跳过头部
      .map(line => (line.split(",")(82).toInt, (line.split(",")(7).toInt,1)))
      .reduceByKey((a,b)=> (a._1+b._1, a._2+b._2))
      .sortByKey()
      .map { case (location, sum) => s"$location,${(1.0*sum._1/sum._2).formatted("%.2f")}" }
      .coalesce(1)
      .saveAsTextFile("src/main/resources/text")
  }
  private def avgExpend(dataRDD: RDD[String]): Unit = {

    dataRDD
      .filter(line => !line.startsWith("患者编号")) // 跳过头部
      .map(line => (line.split(",")(82).toInt, (line.split(",")(57).toFloat, 1)))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .sortByKey()
      .map { case (location, sum) => s"$location,${(1.0 * sum._1)}" }
      .coalesce(1)
      .saveAsTextFile("src/main/resources/text1")
  }
}
