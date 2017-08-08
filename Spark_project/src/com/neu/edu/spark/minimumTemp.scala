package com.neu.edu.spark

import org.apache.log4j._
import org.apache.spark.SparkContext
import scala.math.min

object MinimumTemp {

  def parseLine(line: String)=
  {
val fields=line.split(",")
    val stationId=fields(0)
    val entryType=fields(2)
    val temperature=fields(3).toFloat*0.1f*(0.9f/5.0f)+32.0f
    (stationId,entryType,temperature)
  }

  def main(array: Array[String])=
  {
   Logger.getLogger("org").setLevel(Level.ERROR)

    val sc= new SparkContext("local[*]","MinimumTemp")

    val lines=sc.textFile("./1800.csv")

    val parsedLines=lines.map(parseLine)

    val minTemp=parsedLines.filter(x=>x._2=="TMIN")

    val stationTemp=minTemp.map(x=>(x._1,x._3))

    val minTempsByStation = stationTemp.reduceByKey( (x,y) => min(x,y))

    val result=minTempsByStation.collect()

    for (result <- result.sorted) {
      val station = result._1
      val temp = result._2
      val formattedTemp = f"$temp%.2f F"
      println(s"$station minimum temperature: $formattedTemp")
    }


  }
}
