package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestLtExpression {
  @Test
  @DisplayName("$lt arrays")
  void arrays() {
    assertEquals(
        v(false),
        function(o(f("$lt", a(v("$test"), a(v("test"), v(0)))))).apply(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$lt objects")
  void objects() {
    assertEquals(
        v(false),
        function(o(f("$lt", a(v("$test"), o(f("$literal", o(f("test", v(0)))))))))
            .apply(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$lt values")
  void values() {
    assertEquals(v(true), function(o(f("$lt", a(v("$test"), v(1))))).apply(o(f("test", v(0)))));
    assertEquals(v(false), function(o(f("$lt", a(v("$test"), v(0))))).apply(o(f("test", v(0)))));
    assertEquals(v(false), function(o(f("$lt", a(v("$test"), v(0))))).apply(o(f("test", v(null)))));
    assertEquals(v(false), function(o(f("$lt", a(v("$test"), v(0))))).apply(o(f("test2", v(0)))));
    assertEquals(
        v(true), function(o(f("$lt", a(v("$test"), v("test"))))).apply(o(f("test", v("rest")))));
    assertEquals(
        v(false), function(o(f("$lt", a(v("$test"), v("test"))))).apply(o(f("test", v("test")))));
    assertEquals(
        v(false),
        function(o(f("$lt", a(v("$a.b"), v(false))))).apply(o(f("a", o(f("b", v(true)))))));
    assertEquals(
        v(true),
        function(o(f("$lt", a(v("$a.b"), v(true))))).apply(o(f("a", o(f("b", v(false)))))));
    assertEquals(
        v(false),
        function(o(f("$lt", a(v("$a.b"), v(true))))).apply(o(f("a", o(f("b", v(true)))))));
    assertEquals(
        v(false),
        function(o(f("$lt", a(v("$a.b"), v(false))))).apply(o(f("a", o(f("b", v(false)))))));
  }
}
