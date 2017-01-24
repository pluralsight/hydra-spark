package hydra.spark

import hydra.spark.dispatch.SparkDispatch

/**
  * Created by alexsilva on 1/20/17.
  */
object DslTest extends  App {

  val dsl =
    s"""
   dispatch {
       |  version = 1
       |
 |  spark.master = "local[*]"
       |
 |  source {
       |    json-file {
       |      path = /Users/alexsilva/Development/hydra-spark/examples/json/data.json
       |    }
       |  }
       |  operations {
       |    publish-to-kafka {
       |      topic = exp.engineering_kpis.leankit.Board
       |      format = avro
       |      key = "key"
       |      properties {
       |        group.id = dave.kpis
       |        serializer.class = io.confluent.kafka.serializers.KafkaAvroEncoder
       |        key.serializer.class = kafka.serializer.StringEncoder
       |        metadata.broker.list = "10.107.159.231:9092,10.107.157.92:9092,10.107.154.150:9092,10.107.152.221:9092,10.107.148.76:9092"
       |        schema.registry.url = "http://172.16.100.49:8081"
       |      }
       |    }
       |  }
       |}
    """.stripMargin

  SparkDispatch.apply(dsl).run()

}
