package com.stu

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.window._
import org.apache.spark.sql.expressions.Window

object StudentReport {

  def main(args: Array[String]): Unit = {


    val student=new StudentCreater()
    student.createStudent()

    val spark=SparkSession.builder().master("local").appName("student data Collector ").getOrCreate()
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    val studentDF=spark.read
      .option("header","true")
      .option("inferschema","true")
      .csv("F:\\SPARK PROJECTS\\StudentProject\\src\\main\\resources\\student.csv")
    //studentDF.printSchema()
    //studentDF.createOrReplaceTempView("student")

    println("Rank of students by Total mark :")
    val studentRank=studentDF.groupBy("studentId").sum("marks").orderBy("studentId").toDF("studentId","Total")
    studentRank.withColumn("Total_Rank",rank().over(Window.orderBy(desc("Total")))).show()


    println("Top three highest scorers for each subject:")
    studentDF.withColumn("Subject_Rank",rank().over(Window.partitionBy("subject")
      .orderBy(desc("marks")))).where(col("Subject_Rank")<=3).show()


    println("Maximum mark scored  per subject :")
    studentDF.groupBy("subject"). max("marks").show()
    println("Minimun mark scored per subject :")
    studentDF.groupBy("subject").min("marks").show()
    println("Avarage marks per subject:")
    studentDF.groupBy("subject").avg("marks").show()

}

}
