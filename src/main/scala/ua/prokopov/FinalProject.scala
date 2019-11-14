package ua.prokopov

import java.nio.file.Paths

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object FinalProject {

  val decimalType: DecimalType = DataTypes.createDecimalType(15, 2)

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Spark Final Project")
      .config("spark.master", "local")
      .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    implicit val amountContext = Context[Amount](Paths.get(getClass.getResource("/amount.csv").toURI).toString,
      StructType(Array(
        StructField("positionId", LongType, false),
        StructField("amount", decimalType, false),
        StructField("eventTime", LongType, false))))
    implicit val warehousePosesContext = Context[WarehousePosition](Paths.get(getClass.getResource("/warehouse_positions.csv").toURI).toString,
      StructType(Array(
        StructField("positionId", LongType, false),
        StructField("warehouse", StringType, false),
        StructField("product", StringType, false),
        StructField("eventTime", LongType, false))))


    val amountDS = read[Amount].as[Amount]
    val whPosDS = read[WarehousePosition].as[WarehousePosition]
    amountDS.show
    whPosDS.show
    currentAmount(amountDS, whPosDS).show
    minMaxAvg(amountDS, whPosDS).show
  }

  def read[T]()(implicit context: Context[T]): DataFrame = spark.read.schema(context.schema).csv(context.path)

  def currentAmount(amountDS: Dataset[Amount], whPosDS: Dataset[WarehousePosition]): Dataset[CurrentAmount] = {
    amountDS.join(whPosDS, amountDS("positionId") === whPosDS("positionId"))
      .groupBy(amountDS("positionId") as "tempPositionId")
      .agg(max(amountDS("eventTime") )as "maxTime")
      .join(amountDS, 'tempPositionId === amountDS("positionId") && $"maxTime" === amountDS("eventTime"))
      .joinWith(whPosDS, amountDS("positionId") === whPosDS("positionId"))
      .map(row => CurrentAmount(
        row._2.positionId,
        row._2.warehouse,
        row._2.product,
        row._1.getAs[java.math.BigDecimal]("amount")
      )).orderBy('positionId)
  }

  def minMaxAvg(amountDS: Dataset[Amount], whPosDS: Dataset[WarehousePosition]): Dataset[MinMaxAvg] = {
    amountDS.groupBy(amountDS("positionId"))
      .agg(min(amountDS("amount")) as "min",
        avg(amountDS("amount"))as "avg",
        max(amountDS("amount")) as "max")
      .joinWith(whPosDS, amountDS("positionId") === whPosDS("positionId"))
      .map(it => MinMaxAvg(
        it._2.warehouse,
        it._2.product,
        it._1.getAs[java.math.BigDecimal]("min"),
        it._1.getAs[java.math.BigDecimal]("avg"),
        it._1.getAs[java.math.BigDecimal]("max"),
      ))
  }

}

case class Context[T](path: String, schema: StructType)
