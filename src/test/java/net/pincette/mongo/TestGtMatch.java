package net.pincette.mongo;

import static com.mongodb.client.model.Filters.gt;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Match.predicate;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestGtMatch {
  @Test
  @DisplayName("$gt arrays")
  void arrays() {
    assertFalse(predicate(gt("test", fromJson(a(v("test"), v(0))))).test(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$gt objects")
  void objects() {
    assertFalse(predicate(gt("test", fromJson(o(f("test", v(0)))))).test(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$gt values")
  void values() {
    assertTrue(predicate(gt("test", 0)).test(o(f("test", v(1)))));
    assertFalse(predicate(gt("test", 0)).test(o(f("test", v(0)))));
    assertFalse(predicate(gt("test", 0)).test(o(f("test", v(null)))));
    assertFalse(predicate(gt("test", 0)).test(o(f("test2", v(0)))));
    assertTrue(predicate(gt("test", "test")).test(o(f("test", v("zest")))));
    assertFalse(predicate(gt("test", "test")).test(o(f("test", v("rest")))));
    assertTrue(predicate(gt("a.b", false)).test(o(f("a", o(f("b", v(true)))))));
    assertFalse(predicate(gt("a.b", true)).test(o(f("a", o(f("b", v(false)))))));
    assertFalse(predicate(gt("a.b", true)).test(o(f("a", o(f("b", v(true)))))));
    assertFalse(predicate(gt("a.b", false)).test(o(f("a", o(f("b", v(false)))))));
  }
}
