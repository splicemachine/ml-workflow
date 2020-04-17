package com.splicemachine.mlrunner

import java.net.URI


import ml.combust.mleap.runtime.frame.Transformer
import ml.combust.mleap.runtime.MleapSupport._

object FileRetriever {
  def loadBundle(path: String): Transformer = {
    new URI(path).loadMleapBundle().get.root
  }
}
