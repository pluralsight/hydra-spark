package hydra.spark.server

import java.io.File

import com.google.common.io.Files


trait TestJars {
  val testBinaryBaseDir = "binaries"
  lazy val testJarDir = testBinaryBaseDir + "/jars"
  lazy val testEggDir = testBinaryBaseDir + "/eggs"

  lazy val emptyBinaryDir = testBinaryBaseDir + "/empty"

  //Guaranteed to be in place during test-phase, regardless of what other building/packaging has been done:
  lazy val emptyJar = new File(this.getClass.getClassLoader.getResource("binaries/jars/empty.jar").getFile())
  lazy val emptyEgg = new File(this.getClass.getClassLoader.getResource("binaries/eggs/empty.egg").getFile())


  /**
    * Returns a list of possible jars that match certain rules from a given directory
    *
    * @param baseDir
    * @return
    */
  def getJarsList(baseDir: String): Seq[File] = {
    val dir = new File(this.getClass.getClassLoader.getResource(baseDir).getFile())
    val candidates = dir.listFiles.toSeq
    candidates.filter { file =>
      val path = file.toString
      path.endsWith(".jar") && !path.endsWith("-tests.jar") && !path.endsWith("-sources.jar") &&
        !path.endsWith("-javadoc.jar") && !path.contains("scoverage")
    }
  }

  // Make testJar lazy so to give a chance for overriding of testJarDir to succeed
  lazy val testJar: java.io.File = {
    val allJars = getJarsList(testJarDir)
    assert(allJars.size == 1, allJars.toList.toString)
    allJars.head
  }

  lazy val jarBytes: Array[Byte] = Files.toByteArray(testJar)

  lazy val testEgg: java.io.File = {
    val dir = new java.io.File(testEggDir)
    val eggFiles = dir.listFiles().filter(_.getName.endsWith(".egg")).filter(_.getName.contains("examples"))
    assert(eggFiles.size == 1, eggFiles.toList.toString)
    eggFiles.head
  }
}