/**
  * Created by shriyank on 17/1/17.
  */
package dftry

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object DataFrameTry{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("DataFrameTry")
    val sc = new SparkContext(conf)
    val sqLContext = new SQLContext(sc)
    val personDemographicCSVPath = "/home/shriyank/Programs/SBT/scala_prac/src/main/resources/dataframetry/person-demo.csv"
    val personDemographicReadDF = sqLContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .load(personDemographicCSVPath)

    val personHealthCSVPath = "/home/shriyank/Programs/SBT/scala_prac/src/main/resources/dataframetry/person-health.csv"
    val personHealthDF = sqLContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(personHealthCSVPath)

    val personInsuranceCSVPath = "/home/shriyank/Programs/SBT/scala_prac/src/main/resources/dataframetry/person-insurance.csv"
    val personInsuranceDF = sqLContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load(personInsuranceCSVPath)

//    personDemographicReadDF.show()
//    personHealthDF.show()
//    personInsuranceDF.show()

    // Method 1 for joining of both the dataframes
//    val personDF1 = personDemographicReadDF
//                      .join(personHealthDF, personDemographicReadDF("id") === personHealthDF("id"),
//                      "leftouter"
//                      )

    // Method 2 for joining of both the dataframes  (PREFFERED)
    // it is preferred as we are joining the smaller to the bigger table
    //left should be bigger and right should be smaller
    val personDF = personHealthDF
      .join( broadcast(personDemographicReadDF), personHealthDF("id") === personDemographicReadDF("id"),
        "rightouter"
      ).drop(personHealthDF("id"))
    // keep the bigger data on left and the smaller data on left on joining
    //personDF1.show()
    //personDF.show()

    val perDF = personDF
      .join( broadcast(personInsuranceDF), personDF("id") === personInsuranceDF("id"),
      "right_outer"
      ).drop(personDF("id"))

    println("----------------------------JOIN of person insurance------------------------------------")
    perDF.show()

    val ageLessThan50DF = personDF.filter(personDF("age")<50)
    println("----------------age less than 50------------------------------------------------")
    ageLessThan50DF.show()
    ageLessThan50DF
      .write
      .mode(SaveMode.Overwrite)   // to overwrite the csv if saved to a given directory
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("/home/shriyank/Programs/SBT/scala_prac/opCSV")

    val a = personInsuranceDF.filter(personInsuranceDF("datevalidation").gt(current_date()))
    //a.dropDuplicates(Array("id"))
    //a.show()
    val b = a.groupBy(a("payer")).agg(Map("amount" -> "sum"))
    b.show()
    b.write
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("/home/shriyank/Programs/SBT/scala_prac/sumss")

//    val LICDF = perDF.filter((perDF("payer")=== "lic") && (perDF("datevalidation").gt(current_date())))
//    val AIGDF = perDF.filter((perDF("payer")=== "aig") && (perDF("datevalidation").gt(current_date())))
//
    //drop table usin dropduplicate method

    //LICDF.dropDuplicates(Array("id")).show()

    // drop table using groupBy method
//    val dropLICDF = LICDF.groupBy("id", "amount").agg(max("id"))
//    val dropAIGDF = AIGDF.groupBy("id", "amount").agg(max("id"))
//    println("------------------------concise LIC and AIG Table--------------------------------------------")
//    dropLICDF.show()
    //dropAIGDF.show()
//    LICDF.agg(countDistinct("id")).show()
    //LICDF.groupBy("id").count().show
    //var dropLICDF = LICDF.filter((LICDF("id")).distinct())
//    val dropLICDF = LICDF.select(LICDF("amount")).distinct()
//    dropLICDF
    //dropLICDF.dropDuplicates("id", "amount")
    //dropLICDF.select("id").distinct().collect()
    //println("------------------------LIC valid value--------------------------------------------")
//    val sumlic = dropLICDF.agg(sum("amount")).first.get(0)
//    println(sumlic)

    //val licRDD = sc.parallelize(sumlic.toString)
  //licRDD.coalesce(1).saveAsTextFile("/home/shriyank/Programs/SBT/scala_prac/lic_sums")
//    println("-----------------------------AIG value-----------------------------------------------")
//    val sumaig = dropAIGDF.agg(sum("amount")).first.get(0)
//    println(sumaig)

    //val aigRDD = sc.parallelize(sumaig.toString)
    //aigRDD.coalesce(1).saveAsTextFile("/home/shriyank/Programs/SBT/scala_prac/aig_sums")

  }
}

