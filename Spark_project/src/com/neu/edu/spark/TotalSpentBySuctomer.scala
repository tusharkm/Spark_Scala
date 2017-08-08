package com.neu.edu.spark
import org.apache.log4j._
import org.apache.spark.SparkContext
/**
  * Created by Tushar on 8/8/2017.
  */
object TotalSpentByCuctomer {

  def extractData(lines: String)={
    val fields=lines.split(",")
    (fields(0).toInt,fields(2).toFloat)
  }

  def main(args:Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[1]","TotalSpentByCustomer")

    val input=sc.textFile("./customer-orders.csv")

    val mappedInput=input.map(extractData)

    val sumValues=mappedInput.reduceByKey((x,y)=>x+y)

    val sortedValues=sumValues.map((x)=>(x._2,x._1)).sortByKey()
    for (result <- sortedValues) {
      val total = result._1
      val customeId = result._2
      println(s"$customeId: $total")
    }

   // val result=sumValues.collect()

    //result.foreach(println)
  }

}
