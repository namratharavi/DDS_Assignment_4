package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    var pt = pointString.split(",")

    var ptX = pt(0).trim.toDouble
    var ptY = pt(1).trim.toDouble

    var rectangle = queryRectangle.split(",")

    var rectX1 = rectangle(0).trim.toDouble
    var rectY1 = rectangle(1).trim.toDouble
    var rectX2 = rectangle(2).trim.toDouble
    var rectY2 = rectangle(3).trim.toDouble

    var minX = math.min(rectX1, rectX2)
    var maxX = math.max(rectX1, rectX2)
    var minY = math.min(rectY1, rectY2)
    var maxY = math.max(rectY1, rectY2)

    if(ptX < minX || ptX > maxX || ptY < minY || ptY > maxY)
      return false

    return true
  }

}
