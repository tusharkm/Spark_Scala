package com.neu.edu.spark

import org.apache.log4j._
import org.apache.spark.SparkContext

/**
  * Created by Tushar on 8/7/2017.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc=new SparkContext("local[*]","WordCount")

    val input=sc.textFile("./book.txt")

    // regular expression that extracts words

    val words=input.flatMap(x=>x.split("\\W+"))

    //converting word to lower case

    val lowerWords=words.map(x=>x.toLowerCase())


    val wordsCount=lowerWords.countByValue()

    wordsCount.foreach(println)
  }

}
