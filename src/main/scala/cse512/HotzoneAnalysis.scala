package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object HotzoneAnalysis {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotZoneAnalysis(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = 
  {

    var pointDf = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
    pointDf = pointDf.toDF()
    pointDf.createOrReplaceTempView("point")

    // Parse point data formats
    spark.udf.register("trim",(string : String)=>(string.replace("(", "").replace(")", "")))
    pointDf = spark.sql("select trim(_c5) as _c5 from point")
    pointDf.createOrReplaceTempView("point")
    pointDf.show()

    // Load rectangle data
    var rectangleDf = spark.read.format("csv").option("delimiter","\t").option("header","false").load(rectanglePath);
    rectangleDf = rectangleDf.toDF()
    rectangleDf.createOrReplaceTempView("rectangle")
    rectangleDf.show()

    // Join two datasets
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(HotzoneUtils.ST_Contains(queryRectangle, pointString)))
    val joinDf = spark.sql("select rectangle._c0 as rectangle, point._c5 as point from rectangle, point where ST_Contains(rectangle._c0,point._c5)")
    joinDf.createOrReplaceTempView("joinResult")


    //Group and sort the datasets
    var sortDf = spark.sql("select joinResult.rectangle, count(joinResult.point) as points from joinResult group by rectangle")
    sortDf.orderBy(col("rectangle").asc)
    sortDf.createOrReplaceTempView("result")

    return sortDf.coalesce(1)
  }

}
