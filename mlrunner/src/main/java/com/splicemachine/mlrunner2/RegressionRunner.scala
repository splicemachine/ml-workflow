package com.splicemachine.mlrunner2

import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}

object RegressionRunner {
  def runModel(transformer: Transformer, leapFrame: DefaultLeapFrame): Double = {
    transformer.transform(leapFrame).get.select("prediction").get.dataset.last.getDouble(0)
  }
}
