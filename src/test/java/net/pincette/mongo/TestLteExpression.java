package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestLteExpression {
  @Test
  @DisplayName("$lte arrays")
  void arrays() {
    assertEquals(
        v(false),
        function(o(f("$lte", a(v("$test"), a(v("test"), v(0)))))).apply(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$lte objects")
  void objects() {
    assertEquals(
        v(false),
        function(o(f("$lte", a(v("$test"), o(f("$literal", o(f("test", v(0)))))))))
            .apply(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$lte values")
  void values() {
    assertEquals(v(true), function(o(f("$lte", a(v("$test"), v(1))))).apply(o(f("test", v(0)))));
    assertEquals(v(true), function(o(f("$lte", a(v("$test"), v(0))))).apply(o(f("test", v(0)))));
    assertEquals(v(false), function(o(f("$lte", a(v("$test"), v(0))))).apply(o(f("test", v(1)))));
    assertEquals(
        v(false), function(o(f("$lte", a(v("$test"), v(0))))).apply(o(f("test", v(null)))));
    assertEquals(v(false), function(o(f("$lte", a(v("$test"), v(0))))).apply(o(f("test2", v(1)))));
    assertEquals(
        v(true), function(o(f("$lte", a(v("$test"), v("test"))))).apply(o(f("test", v("rest")))));
    assertEquals(
        v(true), function(o(f("$lte", a(v("$test"), v("test"))))).apply(o(f("test", v("test")))));
    assertEquals(
        v(false), function(o(f("$lte", a(v("$test"), v("test"))))).apply(o(f("test", v("zest")))));
    assertEquals(
        v(false),
        function(o(f("$lte", a(v("$a.b"), v(false))))).apply(o(f("a", o(f("b", v(true)))))));
    assertEquals(
        v(true),
        function(o(f("$lte", a(v("$a.b"), v(true))))).apply(o(f("a", o(f("b", v(false)))))));
    assertEquals(
        v(true),
        function(o(f("$lte", a(v("$a.b"), v(true))))).apply(o(f("a", o(f("b", v(true)))))));
    assertEquals(
        v(true),
        function(o(f("$lte", a(v("$a.b"), v(false))))).apply(o(f("a", o(f("b", v(false)))))));
  }
}
