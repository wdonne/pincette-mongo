package net.pincette.mongo;

import static com.mongodb.client.model.Filters.nin;
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

public class TestNinMatch {
  @Test
  @DisplayName("$nin arrays")
  public void arrays() {
    final JsonArray a1 = a(v(0), v("test"), o(f("test", v(0))));
    final JsonArray a2 = a(v(0), v("test"), o(f("test", v(1))));

    assertTrue(predicate(nin("test", fromJson(a(a2)))).test(o(f("test", a1))));
    assertFalse(predicate(nin("test", fromJson(a(a1, a2)))).test(o(f("test", a1))));
  }

  @Test
  @DisplayName("$nin objects")
  public void objects() {
    final JsonObject o1 = o(f("test", v(0)));
    final JsonObject o2 = o(f("test", v(1)));

    assertTrue(predicate(nin("test", fromJson(a(o2)))).test(o(f("test", o1))));
    assertFalse(predicate(nin("test", fromJson(a(o1, o2)))).test(o(f("test", o1))));
  }

  @Test
  @DisplayName("$nin values")
  @SuppressWarnings("java:S1192")
  public void values() {
    assertTrue(predicate(nin("test", fromJson(a(v(0), v(1))))).test(o(f("test", v(2)))));
    assertFalse(predicate(nin("test", fromJson(a(v(0), v(1))))).test(o(f("test", v(1)))));
    assertTrue(predicate(nin("test", fromJson(a(v(0), v(1))))).test(o(f("test", v(null)))));
    assertTrue(predicate(nin("test", fromJson(a(v(0), v(1))))).test(o(f("test2", v(1)))));
    assertTrue(
        predicate(nin("test", fromJson(a(v("test1"), v("test2"))))).test(o(f("test", v("test3")))));
    assertFalse(
        predicate(nin("test", fromJson(a(v("test1"), v("test2"))))).test(o(f("test", v("test1")))));
    assertFalse(
        predicate(nin("a.b", fromJson(a(v(true), v(false))))).test(o(f("a", o(f("b", v(true)))))));
    assertFalse(
        predicate(nin("a.b", fromJson(a(v(true), v(false))))).test(o(f("a", o(f("b", v(false)))))));
    assertTrue(predicate(nin("a.b", fromJson(a(v(true))))).test(o(f("a", o(f("b", v(false)))))));
    assertTrue(predicate(nin("a.b", fromJson(a(v(false))))).test(o(f("a", o(f("b", v(true)))))));
  }
}
