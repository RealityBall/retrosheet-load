package org.bustos.realityball

import org.bustos.realityball.RealityballRecords._

import scala.collection.mutable.Queue
import scala.math.{ pow, sqrt }
import RealityballRecords._

trait StatisticsTrait {

  def movingAverageSimple(data: Queue[Statistic]): Statistic = {
    val runningSum = data.foldLeft(Statistic(0.0, 0.0, 0.0))({ (x, y) => Statistic(accum(x.total, y.total), accum(x.rh, y.rh), accum(x.lh, y.lh)) })
    val runningCounts = data.foldLeft(Statistic(0.0, 0.0, 0.0))({ (x, y) => Statistic(accumCount(x.total, y.total), accumCount(x.rh, y.rh), accumCount(x.lh, y.lh)) })
    if (data.size > 0) Statistic(runningSum.total.toDouble / runningCounts.total.toDouble,
      runningSum.rh.toDouble / runningCounts.rh.toDouble,
      runningSum.lh.toDouble / runningCounts.lh.toDouble)
    else Statistic(Double.NaN, Double.NaN, Double.NaN)
  }

  def movingAverage(data: Queue[StatisticInputs]): Statistic = {
    val runningSum = data.foldLeft(StatisticInputs(0, 0, 0, 0, 0, 0))({ (x, y) =>
      StatisticInputs(x.totalNumer + y.totalNumer, x.totalDenom + y.totalDenom,
        x.rhNumer + y.rhNumer, x.rhDenom + y.rhDenom,
        x.lhNumer + y.lhNumer, x.lhDenom + y.lhDenom)
    })
    if (data.size > 0) Statistic(if (runningSum.totalDenom > 0.0) runningSum.totalNumer.toDouble / runningSum.totalDenom.toDouble else Double.NaN,
      if (runningSum.rhDenom > 0.0) runningSum.rhNumer.toDouble / runningSum.rhDenom.toDouble else Double.NaN,
      if (runningSum.lhDenom > 0.0) runningSum.lhNumer.toDouble / runningSum.lhDenom.toDouble else Double.NaN)
    else Statistic(Double.NaN, Double.NaN, Double.NaN)
  }

  def movingVolatilitySimple(data: Queue[Statistic]): Statistic = {
    val average = movingAverageSimple(data)
    val runningAccum = data.foldLeft(Statistic(0.0, 0.0, 0.0))({ (x, y) => Statistic(x.total + pow(y.total - average.total, 2.0), x.rh + pow(y.rh - average.rh, 2.0), x.lh + pow(y.lh - average.lh, 2.0)) })
    val runningCounts = data.foldLeft(Statistic(0.0, 0.0, 0.0))({ (x, y) => Statistic(accumCount(x.total, y.total), accumCount(x.rh, y.rh), accumCount(x.lh, y.lh)) })
    Statistic(sqrt(runningAccum.total / runningCounts.total), sqrt(runningAccum.rh / runningCounts.rh), sqrt(runningAccum.lh / runningCounts.lh))
  }

  def movingVolatility(data: Queue[StatisticInputs]): Statistic = {
    val average = movingAverage(data)
    val runningTotal = data.foldLeft((0.0, 0))({ (x, y) =>
      {
        if (y.totalDenom > 0) (x._1 + pow(y.totalNumer.toDouble / y.totalDenom.toDouble - average.total, 2.0), x._2 + 1)
        else x
      }
    })
    val runningRH = data.foldLeft((0.0, 0))({ (x, y) =>
      {
        if (y.rhDenom > 0) (x._1 + pow(y.rhNumer.toDouble / y.rhDenom.toDouble - average.rh, 2.0), x._2 + 1)
        else x
      }
    })
    val runningLH = data.foldLeft((0.0, 0))({ (x, y) =>
      {
        if (y.lhDenom > 0) (x._1 + pow(y.lhNumer.toDouble / y.lhDenom.toDouble - average.lh, 2.0), x._2 + 1)
        else x
      }
    })
    Statistic(sqrt(runningTotal._1 / runningTotal._2.toDouble), sqrt(runningRH._1 / runningRH._2.toDouble), sqrt(runningLH._1 / runningLH._2.toDouble))
  }

  def mean(data: Queue[Double]): Double = {
    if (data.size == 0) Double.NaN
    else data.reduceLeft(_ + _) / data.size.toDouble
  }

  def standardDeviation(data: Queue[Double]): Double = {
    val dataMean = mean(data)
    data.foldLeft(0.0) { (x, y) => x + pow(y - dataMean, 2.0) } / data.size.toDouble
  }

  def accum(x: Double, y: Double): Double = {
    if (!y.isNaN) {
      if (x.isNaN) y
      else x + y
    } else x
  }

  def accumCount(x: Double, y: Double): Double = {
    if (!y.isNaN) {
      if (x.isNaN) 1
      else x + 1
    } else x
  }
}
