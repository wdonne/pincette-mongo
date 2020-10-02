package net.pincette.mongo;

import static com.mongodb.client.model.Filters.gte;
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

class TestGteMatch {
  @Test
  @DisplayName("$gte arrays")
  void arrays() {
    assertFalse(predicate(gte("test", fromJson(a(v("test"), v(0))))).test(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$gte objects")
  void objects() {
    assertFalse(predicate(gte("test", fromJson(o(f("test", v(0)))))).test(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$gte values")
  void values() {
    assertTrue(predicate(gte("test", 0)).test(o(f("test", v(1)))));
    assertTrue(predicate(gte("test", 0)).test(o(f("test", v(0)))));
    assertFalse(predicate(gte("test", 1)).test(o(f("test", v(0)))));
    assertFalse(predicate(gte("test", 1)).test(o(f("test", v(null)))));
    assertFalse(predicate(gte("test", 1)).test(o(f("test2", v(null)))));
    assertTrue(predicate(gte("test", "test")).test(o(f("test", v("zest")))));
    assertTrue(predicate(gte("test", "test")).test(o(f("test", v("test")))));
    assertFalse(predicate(gte("test", "test")).test(o(f("test", v("rest")))));
    assertTrue(predicate(gte("a.b", false)).test(o(f("a", o(f("b", v(true)))))));
    assertTrue(predicate(gte("a.b", false)).test(o(f("a", o(f("b", v(false)))))));
    assertTrue(predicate(gte("a.b", true)).test(o(f("a", o(f("b", v(true)))))));
    assertFalse(predicate(gte("a.b", true)).test(o(f("a", o(f("b", v(false)))))));
  }
}
