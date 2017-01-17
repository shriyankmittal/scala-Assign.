package rddtry

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * first spark program
  * Created by shriyank on 17/1/17.
  */
object RDDTryOne {
  def main(args: Array[String]): Unit = {

    //val resourcePath = getClass.getResource("").getPath

    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDTryOne")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //reading text file
    val ipPath = "/home/shriyank/Desktop/war.txt"
    val readRDD = sc.textFile(ipPath)
      .cache() // to bring it to memory so as to avoid multiple read as readRdd is used in both
    // to create sampleRDD as welll as doing the count in both the sampleRDD count and readRDD count

    //displaying initial 10 lines
    //readRDD.take(10).foreach(println) // action

    //OR

    var xRDD = readRDD.take(10)
    //xRDD.foreach(println)

    //val sampleRDD = readRDD.sample(withReplacement = false, 0.01,123) // transformation sample will take 0.1 = 10%,
    // creates RDD, lazy evaluation will be executed when the action is called upon
    // 0.1 deict the 10 % of the original RDD file and with sample we, will always ose percentage
    // 123 is the seed value which makes the value output same for every random generation
    //sampleRDD.foreach(println)

    //var z = readRDD.takeSample(withReplacement = false, 10, 123) // action

    // will create an array of the first 10 lines and will be executed when the action is called


    //println("Total Count " + readRDD.count())
    //println("SampleCount " + sampleRDD.count())
    // RDD of list of strings
    val splitRDD = readRDD.flatMap(x => x.toLowerCase.split(" ")).cache()
    splitRDD.take(10).foreach(println)

    // filter stop words

    val stopWordList = List("the", "is", "are", "a", "an")
    println("Total output word Count" + splitRDD.count())

    val filteredRDD = splitRDD.filter(x=> !stopWordList.contains(x))
    println("Filtered Word Count:" + filteredRDD.count())
    //word count
    val wordUnitRDD = filteredRDD.map(x=>(x,1))
    val wordCountRDD = wordUnitRDD
      .groupBy(x=>x._1)
        .map(x=>{
          val key = x._1
          val totalCount = x._2.size
          (key, totalCount)
        })

    //wordCountRDD.foreach(println)

    val freqUnitRDD = wordCountRDD.map(x=>(x._2, 1))
    val freqCountRDD = freqUnitRDD
      .groupBy(x=>x._1)
      .map(x=>{
        val k = x._1
        val t = x._2.size
        (k, t)
      })
    freqCountRDD.foreach(println)
    //freqCountRDD.coalesce(1).saveAsTextFile("/home/shriyank/Programs/SBT/scala_prac/freqs")

    val ll = freqCountRDD.count().toInt / 2
    val perUnitRDD = wordCountRDD.coalesce(1).sortBy(_._2, ascending = false).take(ll)
    //perUnitRDD.foreach(println)
    //perUnitRDD.saveAsTextFile("/home/shriyank/Desktop/WordssSorted")
    //perUnitRDD.saveAsTextFile("/home/shriyank/Programs/SBT/scala_prac/perUnits")
    perUnitRDD.foreach(println)


  }



}
