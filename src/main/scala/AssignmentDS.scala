import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.expressions.UserDefinedFunction

object AssignmentDS extends App {
  val spark = SparkSession.builder().appName("DS_practice").master("local[*]").getOrCreate()

  import spark.implicits._

  val baseDS=spark.read.option("header","true").csv("src/main/resources/Fire_Department_Calls_for_Service.csv").cache()
  baseDS.show()
  //Question 1
  val diff_calls=baseDS.select("Call Type").distinct()
  diff_calls.show()
  println("count of call type" + diff_calls.count())
  //Question 2
  val incident_on_each_call=baseDS.groupBy("Call Type").count()
  incident_on_each_call.show()
  //Question 3
  val count_of_year_calls = baseDS.select("Call Date").map(x=>x.getString(0).split("/")(2)).distinct().count()
  println("Count of years of Fire Service Call in dataset "+count_of_year_calls)
  //Question 4
  implicit val dateEncoders = Encoders.DATE

  val dateUdf: UserDefinedFunction = udf((date: String) =>{
  val formatter = new SimpleDateFormat("MM/dd/yyyy")
    new java.sql.Date(formatter.parse(date).getTime)
  })
  //Questin 4
  val dateDataset = baseDS.select("Call Date")
  val orderedDateDataset = dateDataset.withColumn("Call Date", dateUdf(dateDataset("Call Date"))).as[Date].orderBy($"Call Date".desc)
  val latestDate = orderedDateDataset.first()
  val previousDate = new Date(latestDate.getTime - (7 * 24 * 3600 * 1000).toLong)
  val last_seven_days_call=orderedDateDataset.filter(date => date.getTime > previousDate.getTime).count()
  println("Last sever days service call logs "+last_seven_days_call)
  //Question 5
  val districtData1 = baseDS.select("City", "Neighborhood  District", "Call Date").filter(x => x(0) == "San Francisco" && x.getString(2).split("/")(2).toInt == 2015)
  val neighbour_most_call=districtData1.groupBy("Neighborhood  District").count().orderBy($"count".desc).select("Neighborhood  District").first().getString(0)
  println("Neighbourhood in SF the most generated calls "+neighbour_most_call)
}