package org.bustos.realityball

import org.bustos.realityball.RealityballRecords._

import scala.collection.mutable.Queue
import scala.math.{ pow, sqrt }
import RealityballRecords._
import RealityballConfig._

trait StatisticsTrait {

  private def volatilityContribution(mean: Double, observation: Double, index: Double, count: Int, weighted: Boolean): Double = {
    if (weighted) pow((observation - mean) * ((count.toDouble - index) / count.toDouble), 2.0)
    else pow((observation - mean), 2.0)
  }

  private def accum(running: Double, next: Double, runningCount: Double, weighted: Boolean): Double = {
    val factor = if (weighted) MovingAverageExponentialWeights(runningCount.toInt + 1) else 1.0
    if (!next.isNaN) {
      if (running.isNaN) next * factor
      else running + next * factor
    } else running
  }

  private def accumCount(running: Double, next: Double, weighted: Boolean): Double = {
    val factor = if (weighted) MovingAverageExponentialWeights(running.toInt) else 1.0
    if (!next.isNaN) {
      if (running.isNaN) factor
      else running + factor
    } else running
  }

  def queueMeanSimple(data: Queue[Statistic], weighted: Boolean): Statistic = {
    val running = data.foldLeft((Statistic("", 0.0, 0.0, 0.0), Statistic("", 0.0, 0.0, 0.0)))({ (x, y) =>
      (Statistic("", accum(x._1.total, y.total, x._2.total, weighted), accum(x._1.rh, y.rh, x._2.rh, weighted), accum(x._1.lh, y.lh, x._2.lh, weighted)),
        Statistic("", accumCount(x._2.total, y.total, weighted), accumCount(x._2.rh, y.rh, weighted), accumCount(x._2.lh, y.lh, weighted)))
    })
    if (data.size > 0) Statistic("",
      if (running._2.total > 0.0) running._1.total / running._2.total else Double.NaN,
      if (running._2.rh > 0.0) running._1.rh / running._2.rh else Double.NaN,
      if (running._2.lh > 0.0) running._1.lh / running._2.lh else Double.NaN)
    else Statistic("", Double.NaN, Double.NaN, Double.NaN)
  }

  def queueMean(data: Queue[StatisticInputs], weighted: Boolean): Statistic = {
    val running = data.foldLeft((StatisticInputs(0.0, 0.0, 0.0, 0.0, 0.0, 0.0), 0))({ (x, y) =>
      val factor = if (weighted) MovingAverageExponentialWeights(x._2) else 1.0
      (StatisticInputs(
        x._1.totalNumer + y.totalNumer * factor, x._1.totalDenom + y.totalDenom * factor,
        x._1.rhNumer + y.rhNumer * factor, x._1.rhDenom + y.rhDenom * factor,
        x._1.lhNumer + y.lhNumer * factor, x._1.lhDenom + y.lhDenom * factor), x._2 + 1)
    })
    if (data.size > 0) Statistic("",
      if (running._1.totalDenom > 0.0) running._1.totalNumer / running._1.totalDenom else Double.NaN,
      if (running._1.rhDenom > 0.0) running._1.rhNumer / running._1.rhDenom else Double.NaN,
      if (running._1.lhDenom > 0.0) running._1.lhNumer / running._1.lhDenom else Double.NaN)
    else Statistic("", Double.NaN, Double.NaN, Double.NaN)
  }

  def movingVolatilitySimple(data: Queue[Statistic], weighted: Boolean): Statistic = {
    val average = queueMeanSimple(data, false)
    val runningAccum = data.foldLeft((Statistic("", 0.0, 0.0, 0.0), Statistic("", 0.0, 0.0, 0.0)))({ (x, y) =>
      val totalWeight = if (weighted) ((data.size.toDouble - x._2.total) / data.size.toDouble) else 1.0
      val rhWeight = if (weighted) ((data.size.toDouble - x._2.rh) / data.size.toDouble) else 1.0
      val lhWeight = if (weighted) ((data.size.toDouble - x._2.lh) / data.size.toDouble) else 1.0
      (Statistic("",
        x._1.total + volatilityContribution(average.total, y.total, x._2.total, data.size, weighted),
        x._1.rh + volatilityContribution(average.rh, y.rh, x._2.rh, data.size, weighted),
        x._1.lh + volatilityContribution(average.lh, y.lh, x._2.lh, data.size, weighted)),
        Statistic("", x._2.total + totalWeight, x._2.rh + rhWeight, x._2.lh + lhWeight))
    })
    Statistic("", sqrt(runningAccum._1.total / runningAccum._2.total), sqrt(runningAccum._1.rh / runningAccum._2.rh), sqrt(runningAccum._1.lh / runningAccum._2.lh))
  }

  def movingVolatility(data: Queue[StatisticInputs], weighted: Boolean): Statistic = {
    val average = queueMean(data, false)
    val running = data.foldLeft(((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)))({ (x, y) =>
      val total = if (y.totalDenom > 0) {
        val weight = if (weighted) ((data.size.toDouble - x._1._3) / data.size) else 1.0
        (x._1._1 + volatilityContribution(average.total, y.totalNumer.toDouble / y.totalDenom.toDouble, x._1._2, data.size, weighted), x._1._2 + weight, x._1._3 + 1.0)
      } else x._1
      val rh = if (y.rhDenom > 0) {
        val weight = if (weighted) ((data.size.toDouble - x._2._3) / data.size) else 1.0
        (x._2._1 + volatilityContribution(average.rh, y.rhNumer.toDouble / y.rhDenom.toDouble, x._2._2, data.size, weighted), x._2._2 + weight, x._2._3 + 1.0)
      } else x._2
      val lh = if (y.lhDenom > 0) {
        val weight = if (weighted) ((data.size.toDouble - x._3._3) / data.size) else 1.0
        (x._3._1 + volatilityContribution(average.lh, y.lhNumer.toDouble / y.lhDenom.toDouble, x._3._2, data.size, weighted), x._3._2 + weight, x._3._3 + 1.0)
      } else x._3
      (total, rh, lh)
    })
    Statistic("", sqrt(running._1._1 / running._1._2), sqrt(running._2._1 / running._2._2), sqrt(running._3._1 / running._3._2))
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
