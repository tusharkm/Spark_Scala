package com.neu.edu.spark

import org.apache.log4j._
import org.apache.spark.SparkContext
/*Compute teh average number of friends by age */
object FriendsByAge {

  /*function that splits the line and take only age and number of friends tuples*/
  def parseLine (line: String)={
    val fields=line.split(",")
    val age=fields(2).toInt
    val numFriends=fields(3).toInt

    //create tuple
    (age,numFriends)

  }

  def main(args: Array[String]): Unit = {

    //set log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")

    val lines=sc.textFile("./fakefriends.csv")

    val rdd=lines.map(parseLine)

    val totalsByAge = rdd.mapValues(x => (x, 1)).reduceByKey( (x,y) => (x._1 + y._1, x._2 + y._2))

    val averagesByAge = totalsByAge.mapValues(x => x._1 / x._2)

    val results = averagesByAge.collect()

    results.sorted.foreach(println)



  }
}
