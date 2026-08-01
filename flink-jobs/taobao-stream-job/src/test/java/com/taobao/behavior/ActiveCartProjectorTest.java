package com.taobao.behavior;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.taobao.behavior.avro.BehaviorType;
import com.taobao.behavior.model.CartItemState;
import com.taobao.behavior.model.CartMutation;
import com.taobao.behavior.processing.ActiveCartProjector;
import org.junit.jupiter.api.Test;

class ActiveCartProjectorTest {
    @Test
    void repeatedAndStaleTransitionsConverge() {
        var cart = EventTestSupport.event(
                1L, 100L, 10L, BehaviorType.cart, 1_511_658_000_000L, 0L);
        CartItemState active = ActiveCartProjector.nextState(null, cart);
        assertTrue(active.isActive());
        assertEquals(CartMutation.Type.UPSERT, ActiveCartProjector.mutation(cart, active).getType());
        assertNull(ActiveCartProjector.nextState(active, cart));

        var buy = EventTestSupport.event(
                1L, 100L, 10L, BehaviorType.buy, 1_511_658_008_000L, 8L);
        CartItemState removed = ActiveCartProjector.nextState(active, buy);
        assertFalse(removed.isActive());
        assertEquals(CartMutation.Type.DELETE, ActiveCartProjector.mutation(buy, removed).getType());
        assertNull(ActiveCartProjector.nextState(
                removed,
                EventTestSupport.event(
                        1L, 100L, 10L, BehaviorType.cart, 1_511_658_004_000L, 4L)));
    }

    @Test
    void goldenCartSequenceHasOneExactActiveItem() {
        CartItemState item100 = null;
        CartItemState item101 = null;
        var item100Cart = EventTestSupport.event(
                1L, 100L, 10L, BehaviorType.cart, 1_511_658_000_000L, 0L);
        item100 = ActiveCartProjector.nextState(item100, item100Cart);
        var item101Cart = EventTestSupport.event(
                1L, 101L, 11L, BehaviorType.cart, 1_511_658_004_000L, 4L);
        item101 = ActiveCartProjector.nextState(item101, item101Cart);
        var repeatedCart = EventTestSupport.event(
                1L, 101L, 11L, BehaviorType.cart, 1_511_658_005_000L, 5L);
        item101 = ActiveCartProjector.nextState(item101, repeatedCart);
        item100 = ActiveCartProjector.nextState(
                item100,
                EventTestSupport.event(
                        1L, 100L, 10L, BehaviorType.buy, 1_511_658_008_000L, 8L));

        assertFalse(item100.isActive());
        assertTrue(item101.isActive());
        CartMutation result = ActiveCartProjector.mutation(repeatedCart, item101);
        assertEquals(101L, result.getItemId());
        assertEquals(1_511_658_004_000L, result.getAddedAtMs());
        assertEquals(1_511_658_005_000L, result.getLastUpdatedAtMs());
    }

    @Test
    void buyWithoutPriorCartIsAConvergentDelete() {
        var buy = EventTestSupport.event(
                2L, 103L, 13L, BehaviorType.buy, 1_511_658_010_000L, 10L);
        CartItemState removed = ActiveCartProjector.nextState(null, buy);
        assertFalse(removed.isActive());
        assertEquals(CartMutation.Type.DELETE, ActiveCartProjector.mutation(buy, removed).getType());
    }
}
