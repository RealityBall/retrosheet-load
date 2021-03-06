/*

    Copyright (C) 2019 Mauricio Bustos (m@bustos.org)

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

*/

package org.bustos.realityball

import org.bustos.realityball.common.RealityballRecords._
import org.bustos.realityball.common.RealityballConfig._
import org.joda.time.DateTime

import scala.collection.mutable.Queue
import scala.math.{ pow, sqrt }

trait StatisticsTrait {

  val placeTime = new DateTime

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
    val running = data.foldLeft((Statistic(placeTime, 0.0, 0.0, 0.0), Statistic(placeTime, 0.0, 0.0, 0.0)))({ (x, y) =>
      (Statistic(placeTime, accum(x._1.total, y.total, x._2.total, weighted), accum(x._1.rh, y.rh, x._2.rh, weighted), accum(x._1.lh, y.lh, x._2.lh, weighted)),
        Statistic(placeTime, accumCount(x._2.total, y.total, weighted), accumCount(x._2.rh, y.rh, weighted), accumCount(x._2.lh, y.lh, weighted)))
    })
    if (data.size > 0) Statistic(placeTime,
      if (running._2.total > 0.0) running._1.total / running._2.total else Double.NaN,
      if (running._2.rh > 0.0) running._1.rh / running._2.rh else Double.NaN,
      if (running._2.lh > 0.0) running._1.lh / running._2.lh else Double.NaN)
    else Statistic(placeTime, Double.NaN, Double.NaN, Double.NaN)
  }

  def queueMean(data: Queue[StatisticInputs], weighted: Boolean): Statistic = {
    val running = data.foldLeft((StatisticInputs(0.0, 0.0, 0.0, 0.0, 0.0, 0.0), 0))({ (x, y) =>
      val factor = if (weighted) MovingAverageExponentialWeights(x._2) else 1.0
      (StatisticInputs(
        x._1.totalNumer + y.totalNumer * factor, x._1.totalDenom + y.totalDenom * factor,
        x._1.rhNumer + y.rhNumer * factor, x._1.rhDenom + y.rhDenom * factor,
        x._1.lhNumer + y.lhNumer * factor, x._1.lhDenom + y.lhDenom * factor), x._2 + 1)
    })
    if (data.size > 0) Statistic(placeTime,
      if (running._1.totalDenom > 0.0) running._1.totalNumer / running._1.totalDenom else Double.NaN,
      if (running._1.rhDenom > 0.0) running._1.rhNumer / running._1.rhDenom else Double.NaN,
      if (running._1.lhDenom > 0.0) running._1.lhNumer / running._1.lhDenom else Double.NaN)
    else Statistic(placeTime, Double.NaN, Double.NaN, Double.NaN)
  }

  def movingVolatilitySimple(data: Queue[Statistic], weighted: Boolean): Statistic = {
    val mean = queueMeanSimple(data, false)
    val runningAccum = data.foldLeft((Statistic(placeTime, 0.0, 0.0, 0.0), Statistic(placeTime, 0.0, 0.0, 0.0)))({ (x, y) =>
      val totalWeight = if (weighted) ((data.size.toDouble - x._2.total) / data.size.toDouble) else 1.0
      val rhWeight = if (weighted) ((data.size.toDouble - x._2.rh) / data.size.toDouble) else 1.0
      val lhWeight = if (weighted) ((data.size.toDouble - x._2.lh) / data.size.toDouble) else 1.0
      (Statistic(placeTime,
        x._1.total + volatilityContribution(mean.total, y.total, x._2.total, data.size, weighted),
        x._1.rh + volatilityContribution(mean.rh, y.rh, x._2.rh, data.size, weighted),
        x._1.lh + volatilityContribution(mean.lh, y.lh, x._2.lh, data.size, weighted)),
        Statistic(placeTime, x._2.total + totalWeight, x._2.rh + rhWeight, x._2.lh + lhWeight))
    })
    Statistic(placeTime, sqrt(runningAccum._1.total / runningAccum._2.total), sqrt(runningAccum._1.rh / runningAccum._2.rh), sqrt(runningAccum._1.lh / runningAccum._2.lh))
  }

  def movingVolatility(data: Queue[StatisticInputs], weighted: Boolean): Statistic = {
    val mean = queueMean(data, false)
    val running = data.foldLeft(((0.0, 0.0, 0.0), (0.0, 0.0, 0.0), (0.0, 0.0, 0.0)))({ (x, y) =>
      val total = if (y.totalDenom > 0) {
        val weight = if (weighted) ((data.size.toDouble - x._1._3) / data.size) else 1.0
        (x._1._1 + volatilityContribution(mean.total, y.totalNumer.toDouble / y.totalDenom.toDouble, x._1._2, data.size, weighted), x._1._2 + weight, x._1._3 + 1.0)
      } else x._1
      val rh = if (y.rhDenom > 0) {
        val weight = if (weighted) ((data.size.toDouble - x._2._3) / data.size) else 1.0
        (x._2._1 + volatilityContribution(mean.rh, y.rhNumer.toDouble / y.rhDenom.toDouble, x._2._2, data.size, weighted), x._2._2 + weight, x._2._3 + 1.0)
      } else x._2
      val lh = if (y.lhDenom > 0) {
        val weight = if (weighted) ((data.size.toDouble - x._3._3) / data.size) else 1.0
        (x._3._1 + volatilityContribution(mean.lh, y.lhNumer.toDouble / y.lhDenom.toDouble, x._3._2, data.size, weighted), x._3._2 + weight, x._3._3 + 1.0)
      } else x._3
      (total, rh, lh)
    })
    if (weighted) Statistic(placeTime, sqrt(running._1._1 / running._1._2), sqrt(running._2._1 / running._2._2), sqrt(running._3._1 / running._3._2))
    else Statistic(placeTime, sqrt(running._1._1 / running._1._3), sqrt(running._2._1 / running._2._3), sqrt(running._3._1 / running._3._3))
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
