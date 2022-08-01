package ai.economicdatasciences.sia.ml

import ai.economicdatasciences.sia.spark.SparkInit.{sc, spark}

import ai.economicdatasciences.sia.ml.MLMatrix.{toBreezeV, toBreezeM, toBreezeD, printMat}

import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.stat.Statistics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.StandardScaler

class Housing {
  // get data
  val housingLines = sc.textFile("./data/housing.data", 6)
  val housingVals = housingLines.map(x => {
    Vectors.dense(x.split(",").map(_.trim.toDouble))
  })

  // some summary stats
  val housingMat = new RowMatrix(housingVals)
  val housingStats = housingMat.computeColumnSummaryStatistics()

  val housingColSims = housingMat.columnSimilarities()

  // convariance
  val housingCovar = housingMat.computeCovariance()

  // transform to labelled points
  val housingData = housingVals.map(x => {
    val a = x.toArray
    LabeledPoint(a(a.length-1), Vectors.dense(a.slice(0, a.length-1)))
  })

  // split data
  val sets = housingData.randomSplit(Array(0.8, 0.2))
  val housingTrain = sets(0)
  val housingValid = sets(1)

  // scale and normalize
  val scaler = new StandardScaler(true, true).fit(housingTrain.map(x => x.features))
}
