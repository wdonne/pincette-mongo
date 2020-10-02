package net.pincette.mongo;

import static com.mongodb.client.model.Filters.lte;
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

class TestLteMatch {
  @Test
  @DisplayName("$lte arrays")
  void arrays() {
    assertFalse(predicate(lte("test", fromJson(a(v("test"), v(0))))).test(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$lte objects")
  void objects() {
    assertFalse(predicate(lte("test", fromJson(o(f("test", v(0)))))).test(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$lte values")
  void values() {
    assertTrue(predicate(lte("test", 1)).test(o(f("test", v(0)))));
    assertTrue(predicate(lte("test", 0)).test(o(f("test", v(0)))));
    assertFalse(predicate(lte("test", 0)).test(o(f("test", v(1)))));
    assertFalse(predicate(lte("test", 0)).test(o(f("test", v(null)))));
    assertFalse(predicate(lte("test", 0)).test(o(f("test2", v(1)))));
    assertTrue(predicate(lte("test", "test")).test(o(f("test", v("rest")))));
    assertTrue(predicate(lte("test", "test")).test(o(f("test", v("test")))));
    assertFalse(predicate(lte("test", "test")).test(o(f("test", v("zest")))));
    assertTrue(predicate(lte("a.b", true)).test(o(f("a", o(f("b", v(false)))))));
    assertTrue(predicate(lte("a.b", false)).test(o(f("a", o(f("b", v(false)))))));
    assertTrue(predicate(lte("a.b", true)).test(o(f("a", o(f("b", v(true)))))));
    assertFalse(predicate(lte("a.b", false)).test(o(f("a", o(f("b", v(true)))))));
  }
}
