package net.pincette.mongo;

import static com.mongodb.client.model.Filters.ne;
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
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TestNeMatch {
  @Test
  @DisplayName("$ne arrays")
  public void arrays() {
    final JsonArray a1 = a(v(0), v("test"), o(f("test", v(0))));
    final JsonArray a2 = a(v(0), v("test"), o(f("test", v(1))));

    assertFalse(predicate(ne("test", fromJson(a1))).test(o(f("test", a1))));
    assertTrue(predicate(ne("test", fromJson(a2))).test(o(f("test", a1))));
  }

  @Test
  @DisplayName("$ne objects")
  public void objects() {
    final JsonObject o1 = o(f("test", v(0)));
    final JsonObject o2 = o(f("test", v(1)));

    assertFalse(predicate(ne("test", fromJson(o1))).test(o(f("test", o1))));
    assertTrue(predicate(ne("test", fromJson(o2))).test(o(f("test", o1))));
  }

  @Test
  @DisplayName("$ne values")
  public void values() {
    assertFalse(predicate(ne("test", 0)).test(o(f("test", v(0)))));
    assertTrue(predicate(ne("test", 0)).test(o(f("test", v(1)))));
    assertTrue(predicate(ne("test", 0)).test(o(f("test", v(null)))));
    assertTrue(predicate(ne("test", 0)).test(o(f("test2", v(1)))));
    assertFalse(predicate(ne("test", "test")).test(o(f("test", v("test")))));
    assertTrue(predicate(ne("test", "test")).test(o(f("test", v("test2")))));
    assertFalse(predicate(ne("a.b", true)).test(o(f("a", o(f("b", v(true)))))));
    assertTrue(predicate(ne("a.b", true)).test(o(f("a", o(f("b", v(false)))))));
  }
}
