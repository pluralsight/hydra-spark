/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.spark.dsl.sources;

import hydra.spark.api.*;
import hydra.spark.dispatch.SparkDispatch$;
import hydra.spark.sources.ElasticSearchSource;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.MockTransportClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.immutable.HashMap$;

import java.io.IOException;
import java.util.List;

/**
 * Created by alexsilva on 8/22/16.
 */

public class ElasticSearchSourceTest extends ESTestCase {

    private static SparkConf SparkConf = new SparkConf().setMaster("local").setAppName("hydra").set("spark.ui" + "" +
            ".enabled", "false").set("spark.local.dir", "/tmp").set("spark.driver.allowMultipleContexts", "false")
            .set("spark.es.nodes", "localhost").set("spark.es.port", "9200");

    private String sample = "{\n" + "\t\"glossary\": {\n" + "\t\t\"title\": \"example\",\n" + "\t\t\"GlossDiv\": {\n"
            + "\t\t\t\"title\": \"S\",\n" + "\t\t\t\"GlossList\": {\n" + "\t\t\t\t\"GlossEntry\": {\n" +
            "\t\t\t\t\t\"ID\": " + "\"SGML\",\n" + "\t\t\t\t\t\"SortAs\": \"SGML\",\n" + "\t\t\t\t\t\"GlossTerm\": "
            + "\"Standard Generalized Markup Language\",\n" + "\t\t\t\t\t\"Acronym\": \"SGML\",\n" +
            "\t\t\t\t\t\"Abbrev\": \"ISO 8879:1986\",\n" + "\t\t\t\t\t\"GlossDef\":" + " {\n" +
            "\t\t\t\t\t\t\"para\": \"A meta-markup language, used to create markup languages such as DocBook.\",\n" +
            "\t\t\t\t\t\t\"GlossSeeAlso\": [\"GML\", \"XML\"]\n" + "\t\t\t\t\t},\n" + "\t\t\t\t\t\"GlossSee\": " +
            "\"markup\"\n" + "\t\t\t\t}\n" + "\t\t\t}\n" + "\t\t}\n" + "\t}\n" + "}";

    private static SparkContext sparkCtx = null;

    private static Client client = null;

    @BeforeClass
    public static void beforeAll() throws IOException {
        sparkCtx = new SparkContext(SparkConf);
        Settings settings = Settings.builder().put("cluster.name", "estest").put("http.enabled", "true").put("path" +
                ".home", java.nio.file.Files.createTempDirectory("elasticsearch_data_").toFile().toString()).build();

        client = new MockTransportClient(settings);
    }

    @AfterClass
    public static void afterAll() {
        if (sparkCtx != null) {
            try {
                sparkCtx.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        client.close();
    }


    @Test(expected = InvalidDslException.class)
    public void shouldNotAllowStreaming() {
        new ElasticSearchSource("table", HashMap$.MODULE$.empty()).createStream(null);
    }

    @Test
    public void shouldBeInvalidIfNoIndexIsSpecified() {
        Source source = new ElasticSearchSource("", HashMap$.MODULE$.empty());
        ValidationResult validation = source.validate();
        List<ValidationError> errors = JavaConverters.seqAsJavaListConverter(((Invalid) validation).errors()).asJava();
        System.out.println(errors);
        Assert.assertEquals(1, errors.size());
    }


    @Test
    public void shouldCreateADFFromAnIndex() {
        client.admin().indices().create(new CreateIndexRequest("hydra")).actionGet(3000L);
        client.index(new IndexRequest("hydra", "create", "1").source(sample)).actionGet(3000L);
        scala.collection.immutable.Map<String, String> props = new scala.collection.immutable.Map.Map1<>("es.read" +
                ".field.as.array.include", "tags");
        Source source = new ElasticSearchSource("hydra", props);
        Assert.assertEquals(Valid.class, source.validate().getClass());
        DataFrame rows = source.createDF(new SQLContext(sparkCtx));
        Assert.assertEquals(1, rows.count());
    }


    public void shouldParseDSLWithElasticSources() {
        String dsl = "{ \"dispatch\": { \"version\": 1, \"spark.es.nodes\": \"localhost\", \"spark.es.nodes.wan" + ""
                + ".only\": true, \"spark.es.port\": \"80\", \"source\": { \"elastic-search\": { \"index\": " +
                "\"notes\", " + "" + "\"properties\": { \"es\": { \"read\": { \"field\": { \"as\": { \"array\": { " +
                "\"include\": " + "\"tags\" } " + "} } } } } } }, \"operations\": { \"publish-to-kafka\": { " +
                "\"topic\": " + "\"hydra_dispatch_notes_demo\", " + "\"properties\": { \"metadata.broker.list\": " +
                "\"localhost:6667\" }" + " } } } }";

        Dispatch d = SparkDispatch$.MODULE$.apply(dsl, Option.apply(null));

        ElasticSearchSource source = (ElasticSearchSource) d.source();

        Assert.assertEquals("notes", source.index());

        scala.collection.immutable.Map<String, String> props = new scala.collection.immutable.Map.Map1<>("es.read" +
                ".field.as.array.include", "tags");

        Assert.assertEquals(props, source.properties());
        Assert.assertEquals(Valid.class, source.validate().getClass());
    }
}


