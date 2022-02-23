package com.expedia
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, concat, desc, expr, first, lag, lead, round}

/**
 * @author ${user.name}
 */
object App {
  def main(args : Array[String]) {

    val spark = SparkSession.builder().master("local").appName("Email_Generator")
      .getOrCreate()

    val df = spark.read.option("header", "true")
      .option("inferschema", "true")
      .csv(getClass().getClassLoader().getResource("data_2022-01-21_03_10_34_PM.csv").getPath)

    //val df1 = df.groupBy("key_id", "created_date", "key_type").count()

    val df1 = df.groupBy("key_type", "created_date", "key_id").count()

    //val df2 = df1.sort(desc("created_date"), desc("key_type"))
    val df2 = df1.sort(desc("key_type"), desc("created_date"))

    //val win = Window.partitionBy("key_id", "created_date").orderBy(desc("created_date"), desc("key_type"))
    val win = Window.partitionBy("key_type").orderBy(desc("key_type"), desc("created_date"))
   // val win = Window.partitionBy("key_type").orderBy(desc("key_type"), desc("created_date"))

   // val df3 = df2.na.fill(0).withColumn("count", (col("count") - lag(col("count"), 1).over(win) / 100))

   // val df3 = df2.na.fill(0).withColumn("count", (col("count").over(win) / 100))
   val df3 = df2.withColumn("count", (col("count") - lead(col("count"), 1).over(win) )/ lead(col("count"), 1).over(win))
     .withColumn("count", round(col("count"),2))
   // val df4 = df2.na.fill(0).withColumn("count1", concat(col("count")+"abc" +lag(col("count"), 1).over(win)))

   // df2.show()
    df3.show()
    //df4.show()

    val result = df3.na.fill(0)
      .groupBy("created_date").pivot("key_type").agg(first("key_id"), first("count"))
      .withColumnRenamed("duaid_first(key_id)", "duaid")
      .withColumnRenamed("duaid_first(count)", "% change")
      .withColumnRenamed("expuserId_first(key_id)", "expuserId")
      .withColumnRenamed("expuserId_first(count)", "% change")
      .withColumnRenamed("guid_first(key_id)", "guid")
      .withColumnRenamed("guid_first(count)", "% change")
      .withColumnRenamed("havid_first(key_id)", "havid")
      .withColumnRenamed("havid_first(count)", "% change")
      .orderBy(desc("created_date"))



 //   df2.show(10)

   // df3.show(10)

 //   result.show(10)

    var obj = new Email_Generator();
    obj.Display(result);
  }

}
