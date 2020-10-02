package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.json.JsonArray;
import javax.json.JsonObject;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestEqExpression {
  @Test
  @DisplayName("$eq arrays")
  void arrays() {
    final JsonArray a1 = a(v(0), v("test"), o(f("test", v(0))));
    final JsonArray a2 = a(v(0), v("test"), o(f("test", v(1))));

    assertEquals(v(true), function(o(f("$eq", a(v("$test"), a1)))).apply(o(f("test", a1))));
    assertEquals(v(false), function(o(f("$eq", a(v("$test"), a2)))).apply(o(f("test", a1))));
  }

  @Test
  @DisplayName("$eq objects")
  void objects() {
    final JsonObject o1 = o(f("test", v(0)));
    final JsonObject o2 = o(f("test", v(1)));

    assertEquals(
        v(true),
        function(o(f("$eq", a(v("$test"), o(f("$literal", o1)))))).apply(o(f("test", o1))));
    assertEquals(
        v(false),
        function(o(f("$eq", a(v("$test"), o(f("$literal", o2)))))).apply(o(f("test", o1))));
  }

  @Test
  @DisplayName("$eq values")
  void values() {
    assertEquals(v(true), function(o(f("$eq", a(v("$test"), v(0))))).apply(o(f("test", v(0)))));
    assertEquals(v(false), function(o(f("$eq", a(v("$test"), v(0))))).apply(o(f("test2", v(0)))));
    assertEquals(v(false), function(o(f("$eq", a(v("$test"), v(0))))).apply(o(f("test", v(1)))));
    assertEquals(v(false), function(o(f("$eq", a(v("$test"), v(0))))).apply(o(f("test", v(null)))));
    assertEquals(
        v(false), function(o(f("$eq", a(v("$test"), v(0))))).apply(o(f("test2", v(null)))));
    assertEquals(
        v(true), function(o(f("$eq", a(v("$test"), v("test"))))).apply(o(f("test", v("test")))));
    assertEquals(
        v(false), function(o(f("$eq", a(v("$test"), v("test"))))).apply(o(f("test", v("test2")))));
    assertEquals(
        v(true), function(o(f("$eq", a(v("$a.b"), v(true))))).apply(o(f("a", o(f("b", v(true)))))));
    assertEquals(
        v(false),
        function(o(f("$eq", a(v("$a.b"), v(true))))).apply(o(f("a", o(f("b", v(false)))))));
  }
}
