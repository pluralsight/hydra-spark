package hydra.spark.util

import java.net.{URL, URLClassLoader}

import org.slf4j.LoggerFactory

/**
  * Created by alexsilva on 1/12/17.
  */
class ContextURLClassLoader(urls: Array[URL], parent: ClassLoader)
  extends URLClassLoader(urls, parent) {

  val logger = LoggerFactory.getLogger(getClass)

  override def addURL(url: URL) {
    if (!getURLs.contains(url)) {
      super.addURL(url)
      logger.info("Added URL " + url + " to ContextURLClassLoader")
    }
  }
}

