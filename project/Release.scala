import sbt.Keys._
import sbt._
import sbtrelease.ReleasePlugin.autoImport.ReleaseTransformations._
import sbtrelease.ReleasePlugin.autoImport._

object HydraSparkRelease {

  import ls.Plugin._
  import LsKeys._

  lazy val implicitlySettings = {
    lsSettings ++ Seq(
      homepage := Some(url("https://github.com/pluralsight/hydra-spark")),
      tags in lsync := Seq("spark", "akka", "kafka", "hydra", "pluralsight"),
      description in lsync := "DSL Language for Apache Spark",
      externalResolvers in lsync := Seq("Hydra Spark Bintray" at "http://dl.bintray.com/hydra-spark/maven"),
      ghUser in lsync := Some("hydra-spark"),
      ghRepo in lsync := Some("hydra-spark"),
      ghBranch in lsync := Some("master")
    )
  }

  val syncWithLs = (ref: ProjectRef) => ReleaseStep(
    check = releaseStepTaskAggregated(LsKeys.writeVersion in lsync in ref),
    action = releaseStepTaskAggregated(lsync in lsync in ref)
  )

  lazy val ourReleaseSettings = Seq(

    releaseProcess := Seq(
      checkSnapshotDependencies,
      runClean,
      runTest,
      inquireVersions,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      publishArtifacts,
      // lsync seems broken, always returning: Error synchronizing project libraries Unexpected response status: 404
      // syncWithLs(thisProjectRef.value),
      setNextVersion,
      commitNextVersion,
      pushChanges
    )
  )

}