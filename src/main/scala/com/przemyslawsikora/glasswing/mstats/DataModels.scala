/*
 * Copyright (C) 2018 Przemyslaw Sikora
 */

package com.przemyslawsikora.glasswing.mstats

import java.sql.Timestamp

case class Measurement(
                        source: String,
                        attribute: String,
                        datetime: String,
                        value: Double
                      )

case class StatisticKey(
                         source: String,
                         attribute: String,
                         dateTime: Timestamp,
                         period: String
                       )

case class StatisticValue(
                           count: Long,
                           min: Double,
                           max: Double,
                           mean: Double,
                           sum: Double,
                           stdDevPop: Double,
                           stdDevSamp: Double,
                           percentiles: List[Double]
                         )

case class Statistic(
                      key: StatisticKey,
                      value: StatisticValue
                    )

object PeriodType extends Enumeration {
  type PeriodType = Value
  val Year = Value("Year")
  val Quarter = Value("Quarter")
  val Month = Value("Month")
  val Week = Value("Week")
  val Day = Value("Day")
  val Hour = Value("Hour")
  val Minute = Value("Minute")
}
