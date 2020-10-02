package net.pincette.mongo;

import static com.mongodb.client.model.Filters.eq;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.BsonUtil.fromJson;
import static net.pincette.mongo.Match.predicate;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import javax.json.JsonArray;
import javax.json.JsonObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestEqMatch {
  @Test
  @DisplayName("$eq arrays")
  void arrays() {
    final JsonArray a1 = a(v(0), v("test"), o(f("test", v(0))));
    final JsonArray a2 = a(v(0), v("test"), o(f("test", v(1))));

    assertTrue(predicate(eq("test", fromJson(a1))).test(o(f("test", a1))));
    assertFalse(predicate(eq("test", fromJson(a2))).test(o(f("test", a1))));
  }

  @Test
  @DisplayName("$eq objects")
  void objects() {
    final JsonObject o1 = o(f("test", v(0)));
    final JsonObject o2 = o(f("test", v(1)));

    assertTrue(predicate(eq("test", fromJson(o1))).test(o(f("test", o1))));
    assertFalse(predicate(eq("test", fromJson(o2))).test(o(f("test", o1))));
  }

  @Test
  @DisplayName("$eq values")
  void values() {
    assertTrue(predicate(eq("test", 0)).test(o(f("test", v(0)))));
    assertFalse(predicate(eq("test", 0)).test(o(f("test2", v(0)))));
    assertFalse(predicate(eq("test", 0)).test(o(f("test", v(1)))));
    assertFalse(predicate(eq("test", 0)).test(o(f("test", v(null)))));
    assertFalse(predicate(eq("test", 0)).test(o(f("test2", v(null)))));
    assertTrue(predicate(eq("test", "test")).test(o(f("test", v("test")))));
    assertFalse(predicate(eq("test", "test")).test(o(f("test", v("test2")))));
    assertTrue(predicate(eq("a.b", true)).test(o(f("a", o(f("b", v(true)))))));
    assertFalse(predicate(eq("a.b", true)).test(o(f("a", o(f("b", v(false)))))));
  }
}
