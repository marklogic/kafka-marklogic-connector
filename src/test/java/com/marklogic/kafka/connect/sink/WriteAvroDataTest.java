package com.marklogic.kafka.connect.sink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.util.FileCopyUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WriteAvroDataTest extends AbstractIntegrationSinkTest {

    /**
     * Verifies that an instance of an Avro-generated class - AvroTestClass - can be serialized to a byte array and
     * then successfully written as a JSON document to MarkLogic.
     *
     * @throws Exception
     */
    @Test
    void writeBinaryAvroData() throws Exception {
        final String TEST_COLLECTION = "write-avro-test";

        AbstractSinkTask task = startSinkTask(
            MarkLogicSinkConfig.DOCUMENT_FORMAT, "json",
            MarkLogicSinkConfig.DOCUMENT_COLLECTIONS, TEST_COLLECTION,
            MarkLogicSinkConfig.DOCUMENT_URI_SUFFIX, ".json"
        );

        byte[] avroData = serializeUsingAvro(new AvroTestClass("Object 1", 1, true));
        final String topic = "topic1";
        final int partition = 1;
        putAndFlushRecords(task, new SinkRecord(topic, partition, null, null, null, avroData, 0, null, null));

        JsonNode doc = readJsonDocument(getUrisInCollection(TEST_COLLECTION, 1).get(0));
        assertEquals("Object 1", doc.get("name").asText());
        assertEquals(1, doc.get("luckyNumber").asInt());
        assertTrue(doc.get("enabled").asBoolean());
    }

    /**
     * Copied from https://www.baeldung.com/java-apache-avro .
     *
     * @param obj
     * @return
     */
    private byte[] serializeUsingAvro(AvroTestClass obj) throws IOException {
        DatumWriter<AvroTestClass> writer = new SpecificDatumWriter<>(AvroTestClass.class);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().jsonEncoder(AvroTestClass.getClassSchema(), stream);
        writer.write(obj, encoder);
        encoder.flush();
        return stream.toByteArray();
    }

    /**
     * This test is included in case you ever need to update the Avro schema for AvroTestClass. Just enable the test
     * and run it with your changes. Then run "./gradlew generateTestAvroJava". This will create a file under
     * ./build/generated-test-avro-java that you can then add to this package.
     *
     * @throws IOException
     */
    @Test
    @Disabled
    void writeSchema() throws IOException {
        Schema mySchema = SchemaBuilder.record("AvroTestClass")
                              .namespace("com.marklogic.kafka.connect.sink")
                              .fields()
                              .name("name").type().stringType().noDefault()
                              .name("luckyNumber").type().intType().noDefault()
                              .name("enabled").type().booleanType().noDefault()
                              .endRecord();

        FileCopyUtils.copy(
            mySchema.toString(true).getBytes(),
            new File(Paths.get("src", "test", "avro").toFile(), "avroTestClass-schema.avsc")
        );
    }

}
