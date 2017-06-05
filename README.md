# hydra-spark

[![Build Status](https://travis-ci.org/pluralsight/hydra-spark.svg?branch=master)](https://travis-ci.org/pluralsight/hydra-spark)
[![codecov](https://codecov.io/gh/pluralsight/hydra-spark/branch/master/graph/badge.svg)](https://codecov.io/gh/pluralsight/hydra-spark)
[![Join the chat at https://gitter.im/pluralsight/hydra](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/pluralsight/hydra-spark?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

hydra-spark provides a declarative and intuitive interface for creating and submitting [Apache Spark] (http://spark-project.org) 
data flow pipelines leveraging the flexibility of Spark's DataFrame API.

This repo contains the complete Hydra Spark project, including unit tests and deploy scripts.

## Features

- *"Declarative Spak Jobs"*: Simple JSON/HOCON based syntax to describe Spark jobs
- Support for Hadoop, Hive, Kafka (both as a source and sink), Elastic Search and many others. See [Sources](doc/sources.md).
- Support for both batch and Streaming jobs using a unified API. 
- Supports different Spark deploy modes (local, yarn-client) which can also be overriden at the DSL level.
- Supports Scala 2.10 and 2.11

## Version Information

| Version     | Spark Version |
|-------------|---------------|
| 0.5.0       | 1.6.2         |
| master      | 1.6.2         |
| spark-2.0   | 2.0           |

For release notes, look in the `notes/` directory.  They should also be up on [notes.implicit.ly](http://notes.implicit.ly/search/spark-jobserver).

We host non-release jars at [Jitpack](https://jitpack.io).

## Getting Started with Hydra Spark

The easiest way to get started is to try the [Docker container](doc/docker.md) which prepackages a Spark distribution with the Hydra Spark DSL assembly included.

Other ways to run:

* Build and run directly from an IDE. IntelliJ instructions follow below. 
* Run using sbt
* Run sbt assembly and copy the jar to the Spark cluster.

## Development mode

The steps below show you how to use hydra-spark with an example DSL, by running Spark in-process.  *This is not an example of usage in production.*

You need to have [SBT](http://www.scala-sbt.org/release/docs/Getting-Started/Setup.html) installed.

### Using an IDE
If you are using a Scala IDE (such as IntelliJ), you can import the project and start by running any of the test specs.  To run a specific DSL <<more docs coming>>

### WordCountExample walk-through

#### Package Jar - Send to Cluster
Docs coming

## Contribution and Development
Contributions via Github Pull Request are welcome.  See the TODO for some ideas.


Profiling software provided by ![](https://www.yourkit.com/images/yklogo.png)

YourKit supports open source projects with its full-featured Java Profiler.
YourKit, LLC is the creator of <a href="https://www.yourkit.com/java/profiler/">YourKit Java Profiler</a>
and <a href="https://www.yourkit.com/.net/profiler/">YourKit .NET Profiler</a>,
innovative and intelligent tools for profiling Java and .NET applications.


## Contact

Please report bugs/problems to:
<https://github.com/pluralsight/hydra-spark/issues>

## License
Apache 2.0, see LICENSE.md

