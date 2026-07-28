package com.taobao.behavior;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.taobao.behavior.avro.BehaviorType;
import com.taobao.behavior.avro.UserBehaviorEvent;
import java.io.ByteArrayOutputStream;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.jupiter.api.Test;

class AvroContractTest {
    @Test
    void generatedSpecificRecordRoundTripsWithCanonicalSchema() throws Exception {
        UserBehaviorEvent original = EventTestSupport.event(
                42L, 500L, 50L, BehaviorType.buy, 1_511_658_020_000L, 7L);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(bytes, null);
        new SpecificDatumWriter<UserBehaviorEvent>(UserBehaviorEvent.class)
                .write(original, encoder);
        encoder.flush();

        UserBehaviorEvent decoded = new SpecificDatumReader<UserBehaviorEvent>(
                        UserBehaviorEvent.class)
                .read(null, DecoderFactory.get().binaryDecoder(bytes.toByteArray(), null));

        assertEquals(original, decoded);
        assertEquals(
                SchemaCompatibility.SchemaCompatibilityType.COMPATIBLE,
                SchemaCompatibility.checkReaderWriterCompatibility(
                                UserBehaviorEvent.getClassSchema(),
                                UserBehaviorEvent.getClassSchema())
                        .getType());
    }
}
