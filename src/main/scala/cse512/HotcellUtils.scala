package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  def plusMinusOne(x1: Int, x2: Int, y1: Int, y2: Int, z1: Int, z2: Int): Boolean = {
    if(x1 - x2 > 1 || x2 - x1 > 1)
      {
        return false
      }
    else if(y1 - y2 > 1 || y2 - y1 > 1)
    {
      return false
    }
    else if(z1 - z2 > 1 || z2 - z1 > 1)
    {
      return false
    }
    else return true
  }

  def zScoreCalculator(x: Int, y: Int, z: Int, adjCellCount: Int, sumHCells: Int, numOfCells: Int, mean: Double, stdDeviation: Double): Double = {
    val topValue = sumHCells.toDouble - (mean * adjCellCount.toDouble)
    val sqrt = Math.sqrt((((numOfCells.toDouble * adjCellCount.toDouble) - (adjCellCount.toDouble * adjCellCount.toDouble)) /
      (numOfCells.toDouble - 1.0).toDouble).toDouble).toDouble
    val bottomValue = stdDeviation * sqrt

    return (topValue / bottomValue).toDouble
  }

  def CountOfNeighbors(x: Int, y: Int, z: Int, XMin: Int, YMin: Int, ZMin: Int, XMax: Int, YMax: Int, ZMax: Int): Int = {
    var c = 0
    println("hello")
    if(x == XMin || x == XMax)
      c += 1

    if(y == YMin || y == YMax)
      c += 1

    if(z == ZMin || z == ZMax)
      c += 1

    if(c == 1) return 17
    else if(c == 2) return 11
    else if(c == 3) return 7
    else return 26
  }

  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART

}
