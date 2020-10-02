package net.pincette.mongo;

import static com.mongodb.client.model.Filters.bitsAllClear;
import static com.mongodb.client.model.Filters.bitsAllSet;
import static com.mongodb.client.model.Filters.bitsAnyClear;
import static com.mongodb.client.model.Filters.bitsAnySet;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Match.predicate;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestBitsMatch {
  @Test
  @DisplayName("$bitsAllClear")
  void bitsAllClearTest() {
    assertTrue(predicate(bitsAllClear("test", 2)).test(o(f("test", v(1)))));
    assertTrue(predicate(bitsAllClear("test", 10)).test(o(f("test", v(5)))));
    assertFalse(predicate(bitsAllClear("test", 10)).test(o(f("test", v(7)))));
    assertTrue(
        predicate(o(f("test", o(f("$bitsAllClear", a(v(1), v(3))))))).test(o(f("test", v(5)))));
    assertFalse(
        predicate(o(f("test", o(f("$bitsAllClear", a(v(1), v(3))))))).test(o(f("test", v(7)))));
  }

  @Test
  @DisplayName("$bitsAllSet")
  void bitsAllSetTest() {
    assertTrue(predicate(bitsAllSet("test", 2)).test(o(f("test", v(2)))));
    assertTrue(predicate(bitsAllSet("test", 10)).test(o(f("test", v(10)))));
    assertFalse(predicate(bitsAllSet("test", 10)).test(o(f("test", v(7)))));
    assertFalse(predicate(bitsAllSet("test", 10)).test(o(f("test", v(2)))));
    assertTrue(
        predicate(o(f("test", o(f("$bitsAllSet", a(v(1), v(3))))))).test(o(f("test", v(10)))));
    assertFalse(
        predicate(o(f("test", o(f("$bitsAllSet", a(v(1), v(3))))))).test(o(f("test", v(7)))));
  }

  @Test
  @DisplayName("$bitsAnyClear")
  void bitsAnyClearTest() {
    assertTrue(predicate(bitsAnyClear("test", 3)).test(o(f("test", v(1)))));
    assertTrue(predicate(bitsAnyClear("test", 3)).test(o(f("test", v(2)))));
    assertTrue(predicate(bitsAnyClear("test", 10)).test(o(f("test", v(2)))));
    assertFalse(predicate(bitsAnyClear("test", 10)).test(o(f("test", v(10)))));
    assertTrue(
        predicate(o(f("test", o(f("$bitsAnyClear", a(v(0), v(1))))))).test(o(f("test", v(1)))));
    assertFalse(
        predicate(o(f("test", o(f("$bitsAnyClear", a(v(1), v(3))))))).test(o(f("test", v(10)))));
  }

  @Test
  @DisplayName("$bitsAnySet")
  void bitsAnySetTest() {
    assertTrue(predicate(bitsAnySet("test", 3)).test(o(f("test", v(1)))));
    assertTrue(predicate(bitsAnySet("test", 3)).test(o(f("test", v(2)))));
    assertTrue(predicate(bitsAnySet("test", 10)).test(o(f("test", v(2)))));
    assertFalse(predicate(bitsAnySet("test", 10)).test(o(f("test", v(0)))));
    assertTrue(
        predicate(o(f("test", o(f("$bitsAnySet", a(v(0), v(1))))))).test(o(f("test", v(1)))));
    assertFalse(
        predicate(o(f("test", o(f("$bitsAnySet", a(v(1), v(3))))))).test(o(f("test", v(4)))));
  }
}
