package net.pincette.mongo;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.nor;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.or;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Match.predicate;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestLogicalMatch {
  @Test
  @DisplayName("$and")
  @SuppressWarnings("java:S1192")
  void andTest() {
    assertTrue(
        predicate(and(gt("test1", 0), lt("test2", 0)))
            .test(o(f("test1", v(1)), f("test2", v(-1)))));
    assertFalse(
        predicate(and(gt("test1", 0), lt("test2", 0)))
            .test(o(f("test1", v(0)), f("test2", v(-1)))));
  }

  @Test
  @DisplayName("$or")
  @SuppressWarnings("java:S1192")
  void orTest() {
    assertTrue(
        predicate(or(gt("test1", 0), lt("test2", 0))).test(o(f("test1", v(1)), f("test2", v(1)))));
    assertFalse(
        predicate(or(gt("test1", 0), lt("test2", 0))).test(o(f("test1", v(0)), f("test2", v(0)))));
  }

  @Test
  @DisplayName("$nor")
  @SuppressWarnings("java:S1192")
  void norTest() {
    assertTrue(
        predicate(nor(gt("test1", 0), lt("test2", 0)))
            .test(o(f("test1", v(-1)), f("test2", v(1)))));
    assertFalse(
        predicate(nor(gt("test1", 0), lt("test2", 0)))
            .test(o(f("test1", v(0)), f("test2", v(-1)))));
  }

  @Test
  @DisplayName("$not")
  @SuppressWarnings("java:S1192")
  void notTest() {
    assertTrue(predicate(not(gt("test", 0))).test(o(f("test", v(0)))));
    assertFalse(predicate(not(gt("test", 0))).test(o(f("test", v(1)))));
  }
}
