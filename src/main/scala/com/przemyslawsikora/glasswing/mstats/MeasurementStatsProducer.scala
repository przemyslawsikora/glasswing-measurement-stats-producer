/*
 * Copyright (C) 2018 Przemyslaw Sikora
 */

package com.przemyslawsikora.glasswing.mstats

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, RelationalGroupedDataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import com.github.plokhotnyuk.jsoniter_scala.macros._
import com.github.plokhotnyuk.jsoniter_scala.core._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

object MeasurementStatsProducer {

  implicit val codec: JsonValueCodec[Measurement] = JsonCodecMaker.make[Measurement](CodecMakerConfig())

  def main(args: Array[String]): Unit = {
    val kafkaAddress = args(0)
    val spark = buildSparkSession()
    spark.sparkContext.setLogLevel("WARN")
    val measurementsDf = getMeasurementsDataFrameFromKafka(spark, kafkaAddress)
    val measurementsDfWithPeriods = getMeasurementsDfWithTimePeriods(measurementsDf)
    Array(
      PeriodType.Year,
      PeriodType.Quarter,
      PeriodType.Month,
      PeriodType.Week,
      PeriodType.Day,
      PeriodType.Hour,
      PeriodType.Minute
    ).foreach(period => {
      val df = groupBySourceAttributePeriod(measurementsDfWithPeriods, s"measurement_timestamp_$period")
      val statisticsDf = computeStatisticsDf(df, s"measurement_timestamp_$period", period)
      val outputDf = prepareOutputDf(statisticsDf, period)
      writeDataFrameToKafka(outputDf, period, kafkaAddress)
    })
    spark.streams.awaitAnyTermination()
  }

  def prepareOutputDf(dfPeriodResult: DataFrame, period: PeriodType.Value): DataFrame = {
    val buildStatisticValue = udf { (count: Long, min: Double, max: Double, mean: Double,
                                     sum: Double, stdDevPop: Double, stdDevSamp: Double,
                                     percentiles: mutable.WrappedArray[Object]) =>
      StatisticValue(count, min, max, mean, sum, stdDevPop, stdDevSamp,
        percentiles.toStream.map { x: Object => Double.unbox(x) }.toList)
    }

    val buildStatistic = udf { (key: GenericRowWithSchema, value: GenericRowWithSchema) =>
      Statistic(
        StatisticKey(
          key.getString(0),
          key.getString(1),
          key.getTimestamp(2),
          key.getString(3)),
        StatisticValue(
          value.getLong(0),
          value.getDouble(1),
          value.getDouble(2),
          value.getDouble(3),
          value.getDouble(4),
          value.getDouble(5),
          value.getDouble(6),
          value.getAs[mutable.WrappedArray[Double]](7).toList
        ))
    }

    dfPeriodResult
      .withColumn("statisticValue", buildStatisticValue(
        col("value_count"),
        col("value_min"),
        col("value_max"),
        col("value_mean"),
        col("value_sum"),
        col("value_stddev_pop"),
        col("value_stddev_samp"),
        col("value_percentiles")
      ))
      .withColumn("statistic", buildStatistic(
        col("statisticKey"),
        col("statisticValue")
      ))
      .withColumn("value", to_json(col("statistic")))
  }

  def buildSparkSession(): SparkSession = {
    SparkSession
      .builder
      .appName("MeasurementStatsProducer")
      .getOrCreate()
  }

  def getMeasurementsDataFrameFromKafka(spark: SparkSession, kafkaAddress: String): DataFrame = {
    val parseMeasurementFromJson = udf { json: String =>
      val body = if (json.startsWith("\"")) json.substring(1, json.length - 1) else json
      readFromArray(body.replaceAll("\\\\\\\"", "\"").getBytes("UTF-8"))
    }
    spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaAddress)
      .option("subscribe", "measurements")
      .load()
      .select(
        col("value").cast("string")
      )
      .withColumn("measurement", parseMeasurementFromJson(col("value")))
      .select(
        col("measurement.source").alias("measurement_source"),
        col("measurement.attribute").alias("measurement_attribute"),
        col("measurement.datetime").alias("measurement_timestamp"),
        col("measurement.value").alias("measurement_value")
      )
  }

  def getMeasurementsDfWithTimePeriods(dataFrame: DataFrame): DataFrame = {
    dataFrame
      .withColumn("measurement_timestamp_year",
        date_trunc("YEAR", col("measurement_timestamp")))
      .withColumn("measurement_timestamp_quarter",
        date_trunc("QUARTER", col("measurement_timestamp")))
      .withColumn("measurement_timestamp_month",
        date_trunc("MONTH", col("measurement_timestamp")))
      .withColumn("measurement_timestamp_week",
        date_trunc("WEEK", col("measurement_timestamp")))
      .withColumn("measurement_timestamp_day",
        date_trunc("DAY", col("measurement_timestamp")))
      .withColumn("measurement_timestamp_hour",
        date_trunc("HOUR", col("measurement_timestamp")))
      .withColumn("measurement_timestamp_minute",
        date_trunc("MINUTE", col("measurement_timestamp")))
  }

  def groupBySourceAttributePeriod(dataFrame: DataFrame, periodColumnName: String): RelationalGroupedDataset = {
    dataFrame
      .groupBy(
        "measurement_source",
        "measurement_attribute",
        periodColumnName
      )
  }

  def computeStatisticsDf(dataFrame: RelationalGroupedDataset, periodColumnName: String,
                          period: PeriodType.Value): DataFrame = {
    val buildStatisticKey = udf { (source: String, attribute: String, dateTime: Timestamp, period: String) =>
      StatisticKey(source, attribute, dateTime, period)
    }
    dataFrame
      .agg(
        count("measurement_value").alias("value_count"),
        min("measurement_value").alias("value_min"),
        max("measurement_value").alias("value_max"),
        mean("measurement_value").alias("value_mean"),
        sum("measurement_value").alias("value_sum"),
        stddev_pop("measurement_value").alias("value_stddev_pop"),
        stddev_samp("measurement_value").alias("value_stddev_samp"),
        expr("percentile_approx(measurement_value, array(0.25, 0.5, 0.75))").alias("value_percentiles")
      )
      .withColumn("statisticKey", buildStatisticKey(
        col("measurement_source"),
        col("measurement_attribute"),
        col(periodColumnName),
        lit(s"$period")
      ))
  }

  def writeDataFrameToKafka(dataFrame: DataFrame, period: PeriodType.Value, kafkaAddress: String): Unit = {
    dataFrame
      .writeStream
      .format("kafka")
      .outputMode("update")
      .option("checkpointLocation", s"/tmp/measurement${period}StatsProducerCheckpoint")
      .option("kafka.bootstrap.servers", kafkaAddress)
      .option("topic", "measurement_stats")
      .start()
  }
}
