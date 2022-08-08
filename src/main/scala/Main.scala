import InsightsGenerator.generateInsights
import Reader.generateSample
import org.apache.spark.sql.AnalysisException

import java.io.FileNotFoundException
import scala.util.{Failure, Success, Try}

object Main extends App {
  println("\n\t\t<------ Big Data project of an eCommerce behavior dataset ------>")

  //Get the success message after execute the generating reports process
  var message:Try[String] = Try(generateInsights(getPath(args)))

  message match {
    case Success(msg:String) => {
      println(msg)
      println(s"\nIrene Delgado, August 2022")
    }
    case Success(p) => println(s"Path is $p")
    case Failure(e:ArrayIndexOutOfBoundsException) => {
      println("\nError: Not enough arguments specified.")
      printInstructions()
    }
    case Failure(e:IllegalArgumentException) => {
      println("\nError: Argument type incorrect.")
      printInstructions()
    }
    case Failure(e:AnalysisException) => {
      println("An error occurred while processing the report. ")
      println(e.getMessage())
      printInstructions()
    }
    case Failure(e) => print(e.getMessage)
  }

  def printInstructions(): Unit = {
    println("\nPlease review the arguments and try again with the correct format:\n")
    println("\t1- The source path of the dataset (the folder of one or more csv files)\n\t2- If it is a sample: 'true' or 'false'")
    System.exit(1)
  }

  //Define the path of the container folder of the dataset sample
  def getPath(args:Array[String]):String =
    if (!args(1).toBoolean) generateSample(args(0), "*.csv",0.3) //If the path is the whole dataset, generate an sample and return the path
    else args(0) //else return the original path
}
