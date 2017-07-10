logLevel := Level.Warn

resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.4")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "1.5.0")

addSbtPlugin("se.marcuslonnberg" % "sbt-docker" % "1.4.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.2.0-M8")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.5")
