package net.pincette.mongo;

import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.type;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Match.predicate;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TestElementMatch {
  @Test
  @DisplayName("$exists")
  public void existsTest() {
    assertTrue(predicate(exists("test", true)).test(o(f("test", v(0)))));
    assertFalse(predicate(exists("test", true)).test(o(f("test2", v(0)))));
    assertTrue(predicate(exists("test", false)).test(o(f("test2", v(0)))));
    assertFalse(predicate(exists("test", false)).test(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$type")
  public void typeTest() {
    assertTrue(predicate(type("test", "array")).test(o(f("test", a(v(0))))));
    assertFalse(predicate(type("test", "array")).test(o(f("test", v(0)))));
    assertTrue(predicate(type("test", "bool")).test(o(f("test", v(true)))));
    assertTrue(predicate(type("test", "bool")).test(o(f("test", v(false)))));
    assertFalse(predicate(type("test", "bool")).test(o(f("test", v(0)))));
    assertTrue(predicate(type("test", "date")).test(o(f("test", v("2020-04-18")))));
    assertFalse(predicate(type("test", "date")).test(o(f("test", v("test")))));
    assertTrue(predicate(type("test", "double")).test(o(f("test", v(0.0)))));
    assertTrue(predicate(type("test", "double")).test(o(f("test", v(0)))));
    assertFalse(predicate(type("test", "double")).test(o(f("test", v("test")))));
    assertTrue(predicate(type("test", "int")).test(o(f("test", v(0)))));
    assertFalse(predicate(type("test", "int")).test(o(f("test", v("test")))));
    assertTrue(predicate(type("test", "long")).test(o(f("test", v(0L)))));
    assertFalse(predicate(type("test", "long")).test(o(f("test", v("test")))));
    assertTrue(predicate(type("test", "null")).test(o(f("test", v(null)))));
    assertFalse(predicate(type("test", "null")).test(o(f("test", v("test")))));
    assertTrue(predicate(type("test", "object")).test(o(f("test", o(f("test", v(0)))))));
    assertFalse(predicate(type("test", "object")).test(o(f("test", v(0)))));
    assertTrue(predicate(type("test", "string")).test(o(f("test", v("test")))));
    assertFalse(predicate(type("test", "string")).test(o(f("test", v(0)))));
    assertTrue(predicate(type("test", "timestamp")).test(o(f("test", v("2020-04-18T00:00:00Z")))));
    assertFalse(predicate(type("test", "timestamp")).test(o(f("test", v("test")))));
    assertFalse(predicate(type("test", "test")).test(o(f("test", v(0)))));
  }
}
