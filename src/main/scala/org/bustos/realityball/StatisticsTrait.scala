package org.bustos.realityball

import org.bustos.realityball.RealityballRecords._

import scala.collection.mutable.Queue
import scala.math.{ pow, sqrt }
import RealityballRecords._

trait StatisticsTrait {

  private def volatilityContribution(mean: Double, observation: Double, index: Double, count: Int, weighted: Boolean): Double = {
    if (weighted) pow((observation - mean) * (index / count.toDouble), 2.0)
    else pow((observation - mean), 2.0)
  }

  private def accum(x: Double, y: Double): Double = {
    if (!y.isNaN) {
      if (x.isNaN) y
      else x + y
    } else x
  }

  private def accumCount(x: Double, y: Double): Double = {
    if (!y.isNaN) {
      if (x.isNaN) x
      else x + 1
    } else x
  }

  def queueMeanSimple(data: Queue[Statistic]): Statistic = {
    val running = data.foldLeft((Statistic(0.0, 0.0, 0.0), Statistic(0.0, 0.0, 0.0)))({ (x, y) =>
      (Statistic(accum(x._1.total, y.total), accum(x._1.rh, y.rh), accum(x._1.lh, y.lh)), Statistic(accumCount(x._2.total, y.total), accumCount(x._2.rh, y.rh), accumCount(x._2.lh, y.lh)))
    })
    if (data.size > 0) Statistic(
      if (running._2.total > 0.0) running._1.total / running._2.total else Double.NaN,
      if (running._2.rh > 0.0) running._1.rh / running._2.rh else Double.NaN,
      if (running._2.lh > 0.0) running._1.lh / running._2.lh else Double.NaN)
    else Statistic(Double.NaN, Double.NaN, Double.NaN)
  }

  def queueMean(data: Queue[StatisticInputs]): Statistic = {
    val running = data.foldLeft(StatisticInputs(0, 0, 0, 0, 0, 0))({ (x, y) =>
      StatisticInputs(x.totalNumer + y.totalNumer, x.totalDenom + y.totalDenom,
        x.rhNumer + y.rhNumer, x.rhDenom + y.rhDenom,
        x.lhNumer + y.lhNumer, x.lhDenom + y.lhDenom)
    })
    if (data.size > 0) Statistic(
      if (running.totalDenom > 0.0) running.totalNumer.toDouble / running.totalDenom.toDouble else Double.NaN,
      if (running.rhDenom > 0.0) running.rhNumer.toDouble / running.rhDenom.toDouble else Double.NaN,
      if (running.lhDenom > 0.0) running.lhNumer.toDouble / running.lhDenom.toDouble else Double.NaN)
    else Statistic(Double.NaN, Double.NaN, Double.NaN)
  }

  def movingVolatilitySimple(data: Queue[Statistic], weighted: Boolean): Statistic = {
    val average = queueMeanSimple(data)
    val runningAccum = data.foldLeft((Statistic(0.0, 0.0, 0.0), Statistic(0.0, 0.0, 0.0)))({ (x, y) =>
      (Statistic(x._1.total + volatilityContribution(average.total, y.total, x._2.total, data.size, weighted),
        x._1.rh + volatilityContribution(average.rh, y.rh, x._2.rh, data.size, weighted),
        x._1.lh + volatilityContribution(average.lh, y.lh, x._2.lh, data.size, weighted)), Statistic(x._2.total + 1.0, x._2.rh + 1.0, x._2.lh + 1.0))
    })
    Statistic(sqrt(runningAccum._1.total / runningAccum._2.total), sqrt(runningAccum._1.rh / runningAccum._2.rh), sqrt(runningAccum._1.lh / runningAccum._2.lh))
  }

  def movingVolatility(data: Queue[StatisticInputs], weighted: Boolean): Statistic = {
    val average = queueMean(data)
    val runningTotal = data.foldLeft((0.0, 0.0))({ (x, y) =>
      {
        if (y.totalDenom > 0) (x._1 + volatilityContribution(average.total, y.totalNumer.toDouble / y.totalDenom.toDouble, x._2, data.size, weighted), x._2 + 1.0)
        else x
      }
    })
    val runningRH = data.foldLeft((0.0, 0.0))({ (x, y) =>
      {
        if (y.rhDenom > 0) (x._1 + volatilityContribution(average.rh, y.rhNumer.toDouble / y.rhDenom.toDouble, x._2, data.size, weighted), x._2 + 1.0)
        else x
      }
    })
    val runningLH = data.foldLeft((0.0, 0.0))({ (x, y) =>
      {
        if (y.lhDenom > 0) (x._1 + volatilityContribution(average.lh, y.lhNumer.toDouble / y.lhDenom.toDouble, x._2, data.size, weighted), x._2 + 1.0)
        else x
      }
    })
    Statistic(sqrt(runningTotal._1 / runningTotal._2), sqrt(runningRH._1 / runningRH._2), sqrt(runningLH._1 / runningLH._2))
  }

  def mean(data: Queue[Double]): Double = {
    if (data.size == 0) Double.NaN
    else data.reduceLeft(_ + _) / data.size.toDouble
  }

  def standardDeviation(data: Queue[Double]): Double = {
    val dataMean = mean(data)
    sqrt(data.foldLeft(0.0) { (x, y) => x + pow(y - dataMean, 2.0) } / data.size.toDouble)
  }

}
