package eooh

import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vectors

import scala.collection._
//import org.apache.spark.ml.classification.{RandomForestClassifier}
import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.ml.regression.GBTRegressor
import org.sameersingh.scalaplot.Implicits._
import org.sameersingh.scalaplot.Style.Color
import org.apache.spark.mllib.evaluation.RegressionMetrics

import com.datawizards.splot.api.implicits._


import scala.collection.mutable

object uuuuh{

  var max:Int = 10000
  var min:Int = -1
  var error:Double = 0.0
  var maxlength = 0



  def main(args: Array[String]){


    /*val x = Seq(1.0,2.0,3.0,4.0,5.0,6.0)
    val y = Seq(9.0,1.0,9.0,2.0,9.0,3.0)
    val z = Seq(9.0,1.0,9.0,2.0,1.0,2.0)*/

    //output(PNG("result/", "test"), xyChart(x -> Seq(Yf(math.sin(_), "hola", color= Color.Black), Yf(math.cos(_), "Adios", color=Color.Gold)), "titulo"))

    //output(PNG("result/", "test"), xyChart(x -> (y, z)))
    //output(PNG("result/", "test"), xyChart(y, "titulo"))



    val conf = new SparkConf().setAppName("TFM").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    import sqlContext.implicits._

    /*
      *
      *
      * Preparando grid
      *
      *
      *
      *
      *
     */

    val delimiter = Seq(34200, 34290, 108910, 109000)// Quite un 0's, por tanto multiplicar por 1000 no por 10000, principalmente
    // por que tiene muchas clases y no puede clasificarlo ñeñeñeñeñe CORRECCION DOS 0s menos
    var front = 3
    var back = 0
    var middle = 0

    var errForestLat = mutable.ArrayBuffer.empty[Double]
    var errForestLon = mutable.ArrayBuffer.empty[Double]
    var errGBTLat = mutable.ArrayBuffer.empty[Double]
    var errGBTLon = mutable.ArrayBuffer.empty[Double]

    var r2ForestLat = mutable.ArrayBuffer.empty[Double]
    var r2ForestLon = mutable.ArrayBuffer.empty[Double]
    var r2GBTLat = mutable.ArrayBuffer.empty[Double]
    var r2GBTLon = mutable.ArrayBuffer.empty[Double]

    var histLength = mutable.ArrayBuffer.empty[Int]
    var histPos = mutable.ArrayBuffer.empty[Int]
    var histPos2 = mutable.ArrayBuffer.empty[Int]
    var histPos3 = mutable.ArrayBuffer.empty[Int]
    var histPos4 = mutable.ArrayBuffer.empty[Int]


    // var errorFrontFMeasure : mutable.HashMap[Double, Array[Double]] = mutable.HashMap.empty[Double, Array[Double]]






    /*



    FUNCIONES UDF



     */



    val convertToVector = udf((lat: Seq[Double], lon: Seq[Double]) => {
      reduceArray(/*sectorization(*/lat, lon)//)
    })

    val getLengthOfArray = udf((array : Seq[Double])=>
    {
      getLength(array)
    })


    val createLabelSector = udf((lat: Double, lon: Double)=>{
      calculateSector(lat, lon, delimiter)
    })


    val transforToVector = udf((features: Seq[Double]) =>{
      Vectors.dense(defineLength(features.asInstanceOf[mutable.WrappedArray[Double]].toArray, front, back, middle))
    })



    /*


    CARGA DE LOS DATOS



     */




    var dataUn = sqlContext.read.format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(/*"/MacAirEna/Users/Loedded/Documents/DatosApacheSpark/dataR.csv"*/"datosR.csv")


    /*

      PrepareData

     */




    var data = dataUn.orderBy(asc("TimeStamp"))
    data = data.select("OrderID", "lat", "lon").groupBy("OrderID")
      .agg(collect_list("lat").alias("lat"),
        first("lat").alias("firstLat"), last("lat").alias("lastLat"),
        collect_list("lon").alias("lon"),
        first("lon").alias("firstLon"), last("lon").alias("lastLon"))

    val latRdd = data.select("OrderID" ,"lat", "lon", "lastLat", "lastLon").map(x => (x.getString(0), x.get(1).asInstanceOf[mutable.WrappedArray[Double]].toArray, x.get(2).asInstanceOf[mutable.WrappedArray[Double]].toArray, x.getDouble(3), x.getDouble(4)))


    var df2 = latRdd.withColumn("featuresLat", convertToVector($"_2", $"_3")).withColumn("featuresLon", convertToVector($"_3", $"_2"))
      .withColumn("ID", $"_1")
      .withColumn("labelLat", $"_4")
      .withColumn("labelLon", $"_5")

      //var df2 = latRdd.withColumn("features", convertToVector($"_2", $"_3")).withColumn("ID", $"_1")
      //  .withColumn("label", createLabelSector($"_4", $"_5"))
    df2 = df2.withColumn("length", getLengthOfArray($"featuresLat"))

    var h = 0
    var limit = 30
    while(h <= maxlength){
      //if(h < limit)
        histLength+=(df2.filter(df2("length") === h).count().toInt)
      /*else if(h < limit*2) //60
        histPos+=(df2.filter(df2("length") === h).count().toInt)
      else if(h < limit*3) //90
        histPos2+=(df2.filter(df2("length") === h).count().toInt)
      else if(h < limit*4) //120
        histPos3+=(df2.filter(df2("length") === h).count().toInt)
      else
        histPos4+=(df2.filter(df2("length") === h).count().toInt)*/
      h = h +1
    }
    histLength.plotHistogram(30)
    /*histLength.buildPlot().bar().titles("De 0 a 30", "tamaño", "Cantidad").display()
    histPos.buildPlot().bar().titles("De 30 a 60",  "tamaño + 30", "Cantidad").display()
    histPos2.buildPlot().bar().titles("De 60 a 90", "tamaño + 60", "Cantidad").display()
    histPos3.buildPlot().bar().titles("De 90 a 120",  "tamaño + 90", "Cantidad").display()
    histPos4.buildPlot().bar().titles("De 120 a 1500", "tamaño + 120", "Cantidad").display()*/

    val df4 = df2.where($"length" >= 50)



    //val forest = new RandomForestClassifier().setLabelCol("label").setFeaturesCol("features").setNumTrees(20)
    //val forest = new RandomForestRegressor().setLabelCol("label").setFeaturesCol("features").setNumTrees(20)

    val forestlat = new RandomForestRegressor().setLabelCol("labelLat").setFeaturesCol("featuresLat").setNumTrees(20)
    val forestlon = new RandomForestRegressor().setLabelCol("labelLon").setFeaturesCol("featuresLon").setNumTrees(20)

    val GBTLat = new GBTRegressor().setLabelCol("labelLat").setFeaturesCol("featuresLat").setMaxIter(20)
    val GBTLon = new GBTRegressor().setLabelCol("labelLon").setFeaturesCol("featuresLon").setMaxIter(20)


    def getError(dataset: Dataset[Row]): mutable.ArrayBuffer[RegressionMetrics] ={

      val df3 = dataset.select("ID", "featuresLat", "featuresLon", "labelLat", "labelLon")
        .withColumn("featuresLat", transforToVector($"featuresLat"))
        .withColumn("featuresLon", transforToVector($"featuresLon"))


      val dataSplit = df3.randomSplit(Array(0.70, 0.20, 0.10))
      val trainData = dataSplit(0)
      val testData = dataSplit(1)
      val laOtraCosa = dataSplit(2)
      val metrics = mutable.ArrayBuffer.empty[RegressionMetrics]


      metrics.append(new RegressionMetrics(forestlat.fit(trainData).transform(laOtraCosa).map(row => (row.getDouble(2), row.getDouble(3))).rdd))
      metrics.append(new RegressionMetrics(forestlon.fit(trainData).transform(laOtraCosa).map(row => (row.getDouble(2), row.getDouble(3))).rdd))
      metrics.append(new RegressionMetrics(GBTLat.fit(trainData).transform(laOtraCosa).map(row => (row.getDouble(2), row.getDouble(3))).rdd))
      metrics.append(new RegressionMetrics(GBTLon.fit(trainData).transform(laOtraCosa).map(row => (row.getDouble(2), row.getDouble(3))).rdd))



      return metrics
    }


    /*var i = 1
    var j = 0

    while(i < 50) {
      front = i
      var metrics = getError(df4)

      errForestLat.append(metrics(0).rootMeanSquaredError)
      errForestLon.append(metrics(1).rootMeanSquaredError)
      errGBTLat.append(metrics(2).rootMeanSquaredError)
      errGBTLon.append(metrics(3).rootMeanSquaredError)

      r2ForestLat.append(metrics(0).r2)
      r2ForestLon.append(metrics(1).r2)
      r2GBTLat.append(metrics(2).r2)
      r2GBTLon.append(metrics(3).r2)

      i = i+1
    }


    val x1 = (1 to errForestLat.length).map(_.toDouble)
    output(PNG("result/", "RMSEForestLat"), xyChart(x1 -> Y(errForestLat, color=Color.Blue), "RMSE Forest Latitude"))
    output(PNG("result/", "RMSEForestLon"), xyChart(x1 -> Y(errForestLon, color=Color.Blue), "RMSE Forest Longitude"))
    output(PNG("result/", "RMSEGBTLat"), xyChart(x1 -> Y(errGBTLat, color=Color.Blue), "RMSE GBT Latitude"))
    output(PNG("result/", "RMSEGBTLon"), xyChart(x1 -> Y(errGBTLon, color=Color.Blue), "RMSE GBT Longitude"))


    output(PNG("result/", "R2ForestLat"), xyChart(x1 -> Y(r2ForestLat, color=Color.Blue), "R2 Forest Latitude"))
    output(PNG("result/", "R2ForestLon"), xyChart(x1 -> Y(r2ForestLon, color=Color.Blue), "R2 Forest Longitude"))
    output(PNG("result/", "R2GBTLat"), xyChart(x1 -> Y(r2GBTLat, color=Color.Blue), "R2 GBT Latitude"))
    output(PNG("result/", "R2GBTLon"), xyChart(x1 -> Y(r2GBTLon, color=Color.Blue), "R2 GBT Longitude"))
*/


    print("HOLA")
  }




  /*


  REDUCE ARRAY


   */

  def reduceArray(lat:Seq[Double], lon:Seq[Double]): Array[Double]={
    var copyArray = lat.asInstanceOf[mutable.WrappedArray[Double]].toArray.clone()
    var copyArrayLon = lon.asInstanceOf[mutable.WrappedArray[Double]].toArray.clone()

    var i = 1
    var newArray = mutable.ArrayBuffer.empty[Double]
    var latn = 0
    var latv = 0
    var lonn = 0
    var lonv = 0
    while(i < copyArray.length) {
      latn = (copyArray(i) * 1000).toInt
      latv = (copyArray(i-1) * 1000).toInt
      lonn = (copyArrayLon(i) * 1000).toInt
      lonv = (copyArrayLon(i-1) * 1000).toInt

      if (latv != latn || lonn != lonv) {
        //println("Alright")
        newArray.append(latn/1000.toDouble)
      }
      //println(copyArray(i))
      i = i + 1
      //println(i)
      //println(newArray.length)
    }

    return newArray.toArray
  }




  /*



  SECTORIZACION


   */

  def sectorization(lat: Seq[Double], lon: Seq[Double], delimiter: Seq[Int]) : Array[Int]={
    var newArray = mutable.ArrayBuffer.empty[Int]
    var maxValue = delimiter(3)-delimiter(2)
    var i = 0
    var longitude : Int = 0
    var latitude : Int = 0

    /*println("#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\nvalor de delimiter")
    println(maxValue)
    println("#\n#\n#\n#\n#\n#\n#\n#\n#\n#\n#\nvalor de delimiter")*/

    while(i < lat.length) {
      longitude = (lon(i)*1000).toInt
      latitude = (lat(i)*1000).toInt
      /*println(longitude)
      println(latitude)*/
      if (longitude >= delimiter(2) && longitude <= delimiter(3) && latitude >= delimiter(0) && latitude <= delimiter(1)) {
        //println((((latitude - delimiter(0)) * maxValue) + (longitude - delimiter(2))))
        //newArray+=((((latitude - delimiter(0)) * maxValue) + (longitude - delimiter(2))))
        newArray.append((((latitude - delimiter(0)) * maxValue) + (longitude - delimiter(2))))
      }
      i = i+1
    }
    //println(newArray.length)
    return newArray.toArray
  }



  /*



  GET LENGHT


   */

  def getLength(array: Seq[Double]) : Int ={
    if(array.length > maxlength)
      maxlength = array.length
    return array.length
  }



  /*



  DEFINE LENGHT Y AUXILIARES


   */


  def defineLength(data: Seq[Double], front: Int, back: Int, middle: Int): Array[Double] ={
    var sum = front+back+middle
    var dataLength = data.length
    var newArray = mutable.ArrayBuffer.empty[Double]
    if(dataLength >= sum){
      if(front != 0)
        newArray.appendAll(addFirstElement(data, 0, front)) //Array, posicion, cantidad
      if(middle != 0){
        newArray.appendAll(addbackElement(data, ((dataLength-1)/2)-1,  (middle/2)-1 ))
        newArray.append(data((dataLength-1)/2))
        newArray.appendAll(addbackElement(data, ((dataLength-1)/2)+1,  (middle/2)-1 ))
      }
      if(back != 0)
        newArray.appendAll(addbackElement(data, dataLength-1, back))
    }

    return newArray.toArray
  }

  def addFirstElement(data: Seq[Double], pos: Int, quantity: Int): Array[Double] ={
    var newArray = mutable.ArrayBuffer.empty[Double]
    for(i <- pos to (pos+(quantity-1))){
      newArray.append(data(i))
    }
    println(newArray.length)
    return newArray.toArray
  }

  def addbackElement(data: Seq[Double], pos: Int, quantity: Int): Array[Double] ={
    var newArray = mutable.ArrayBuffer.empty[Double]
    val posInit = pos-quantity+1
    for(i <- posInit to (pos))
      newArray.append(data(i))
    println(newArray.length)
    return newArray.toArray
  }




  /*

  CALCULAR EL SECTOR


   */




  def calculateSector(lat: Double, lon: Double, delimiter: Seq[Int]): Double ={
    var maxValue = delimiter(3)-delimiter(2)
    var longitude = (lon*1000).toInt
    var latitude = (lat*1000).toInt
    var result = ((((latitude - delimiter(0)) * maxValue) + (longitude - delimiter(2))))
    return result.toDouble
  }
/*def dynamicTimeWarping(array: Seq[Double], refArray : Seq[Double]) : Array[Double] ={
    var copyArray = array.asInstanceOf[mutable.WrappedArray[Double]].toArray.clone()
    var copyRefArray = array.asInstanceOf[mutable.WrappedArray[Double]].toArray.clone()
    var distances = Matrices.zeros(array.length,refArray.length)
    var cost = Matrices.zeros(array.length, refArray.length)
    for(i <- 0 to copyRefArray.length){
      for(j <- 0 to copyArray.length){
        distances(i, j) = scala.math.pow((copyArray(j) - copyRefArray(i)),2)

      }
    }

    cost(0,0) = distances(0,0)
    for(i <- 1 to copyArray.length)
      cost(0,i) = distances(0,i)+cost(0,i-1)

    for(i <- 1 to copyRefArray.length)
      cost(i,0) = distances(i,0)+cost(i-1, 0)

    for(i <- 0 to copyRefArray.length) {
      for (j <- 0 to copyArray.length) {
        cost(i,j) = math.min(math.min(cost(i-1, j-1), cost(i-1,j)), cost(i, j-1))+distances(i,j)
      }
    }

    var mat = mutable.ArrayBuffer.empty[Seq[Int]]
    var i = refArray.length-1
    var j = array.length-1

    while (i > 0 && j > 0){
      if( i == 0)
        j = j -1
      else if(j == 0)
        i = i - 1
      else {
        if(cost(i-1,j) == math.min(cost(i-1, j-1), math.min(cost(i-1, j), cost(i, j-1))))
          i = i -1
        else if(cost(i,j-1) == math.min(cost(i-1, j-1), math.min(cost(i-1, j), cost(i, j-1))))
          j = j-1
        else{
          i = i-1
          j = j-1
        }
        mat.append(Seq(i,j))
      }
    }
    mat.append(Seq(0,0))
    var cost0 = 0.0
    mat.foreach(n => {
      cost0 = cost0 + distances(n(1), n(2))
    })

    return copyArray
  }*/
}
