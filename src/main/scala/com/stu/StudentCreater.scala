package com.stu

import java.io.{BufferedWriter, FileWriter}

import au.com.bytecode.opencsv.CSVWriter
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random

class StudentCreater {

  def createStudent() ={
  val outputFile=new BufferedWriter(new FileWriter("F:\\SPARK PROJECTS\\StudentProject\\src\\main\\resources\\student.csv"))
  val csvWrite=new CSVWriter(outputFile)
  val csvFiled=Array("studentId","subject","marks")

  val subject=List("Physics","Chemistry","Math","Biology")
  val markList=(0 to 100).toList
  val random=new Random()
  var studentList=new ListBuffer[Array[String]]()
  studentList +=csvFiled
  for(i<-1 to 6){
    for(j<-0 to 3){
      studentList +=Array(i.toString,subject(j),markList(random.nextInt(markList.length)).toString())
    }
  }
  csvWrite.writeAll(studentList.toList)
  outputFile.close()
}
}
