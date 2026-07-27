package com.taobao.behavior.sink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.taobao.behavior.model.ActiveCartItem;
import org.junit.jupiter.api.Test;

class RedisCartCodecTest {
    @Test
    void roundTripsTheBoundedActiveCartValue() {
        String encoded = RedisCartCodec.encode(new ActiveCartItem(100L, 500L, 50L, 1_000L, 2_000L));
        RedisCartCodec.DecodedCartItem decoded = RedisCartCodec.decode(encoded);

        assertEquals("50|1000|2000", encoded);
        assertEquals("500", RedisCartCodec.itemField(500L));
        assertEquals(50L, decoded.categoryId());
        assertEquals(1_000L, decoded.addedAtMs());
        assertEquals(2_000L, decoded.lastUpdatedAtMs());
    }

    @Test
    void rejectsMalformedOrOutOfRangeValues() {
        assertThrows(IllegalArgumentException.class, () -> RedisCartCodec.decode("50|1000"));
        assertThrows(IllegalArgumentException.class, () -> RedisCartCodec.decode("0|1000|2000"));
        assertThrows(IllegalArgumentException.class, () -> RedisCartCodec.itemField(0L));
    }
}
