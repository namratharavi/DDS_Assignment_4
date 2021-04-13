package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.createOrReplaceTempView("pickupInfo")
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  var points = spark.sql(s"select x, y, z from pickupInfo where x<= ${maxX} and x>= ${minX} and y<= ${maxY} " +
    s"and y>= ${minY} and z<= ${maxZ} and z>= ${minZ}" )
  points = points.groupBy("x", "y", "z").count()

  points.createOrReplaceTempView("points")

  val mean = points.agg(sum("count") / numCells).first().getDouble(0)

  var sqrdPt = points.agg(sum(pow("count", 2.0))).first().getDouble(0)

  val stdDeviation = Math.sqrt((sqrdPt / numCells.toDouble) - Math.pow(mean, 2.0)).toDouble

  spark.udf.register("CountOfNeighbors", (x: Int, y: Int, z: Int, XMin: Int, YMin: Int, ZMin: Int, XMax: Int, YMax: Int, ZMax: Int)
  => (HotcellUtils.CountOfNeighbors(x, y, z, XMin, YMin, ZMin, XMax, YMax, ZMax)))

  spark.udf.register("plusMinusOne", (x1: Int, x2: Int, y1: Int, y2: Int, z1: Int, z2: Int) => (HotcellUtils.plusMinusOne(x1, x2, y1, y2, z1, z2)))

  val neighbors = spark.sql(s"select CountOfNeighbors(points.x, points.y, points.z, ${minX}, ${minY}, ${minZ}, ${maxX}, ${maxY}, ${maxZ})" +
    " as cOfNeighbor, points.x, points.y, points.z, points.count from points " +
    " where plusMinusOne(points.x, points.x, points.y, points.y, points.z, points.z)"
  )
  neighbors.groupBy("x", "y", "z")

  neighbors.createOrReplaceTempView("neighbors")

  spark.udf.register("zScore", (x: Int, y: Int, z: Int, countOfNeighbor: Int, sumOfHCells: Int, numOfCells: Int, mean: Double, stdDeviation: Double)
  => HotcellUtils.zScoreCalculator(x, y, z, countOfNeighbor, sumOfHCells, numOfCells, mean, stdDeviation))

  var InfoOfAdj = spark.sql("select zScore(x, y, z, cOfNeighbor, count, " + numCells + ", " + mean + ", " + stdDeviation + ") as valueFromFormula, "+
  "x, y, z from neighbors")
  InfoOfAdj.orderBy(col("valueFromFormula").desc)
  InfoOfAdj.createOrReplaceTempView("zScore")

  val result = spark.sql("select x, y, z from zScore")
  result.createOrReplaceTempView("result")

  return result
}
}
