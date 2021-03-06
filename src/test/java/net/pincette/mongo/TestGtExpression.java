package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestGtExpression {
  @Test
  @DisplayName("$gt arrays")
  void arrays() {
    assertEquals(
        v(false),
        function(o(f("$gt", a(v("$test"), a(v("test"), v(0)))))).apply(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$gt objects")
  void objects() {
    assertEquals(
        v(false),
        function(o(f("$gt", a(v("$test"), o(f("$literal", o(f("test", v(0)))))))))
            .apply(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$gt values")
  void values() {
    assertEquals(v(true), function(o(f("$gt", a(v("$test"), v(0))))).apply(o(f("test", v(1)))));
    assertEquals(v(false), function(o(f("$gt", a(v("$test"), v(0))))).apply(o(f("test", v(0)))));
    assertEquals(v(false), function(o(f("$gt", a(v("$test"), v(0))))).apply(o(f("test", v(null)))));
    assertEquals(v(false), function(o(f("$gt", a(v("$test"), v(0))))).apply(o(f("test2", v(0)))));
    assertEquals(
        v(true), function(o(f("$gt", a(v("$test"), v("test"))))).apply(o(f("test", v("zest")))));
    assertEquals(
        v(false), function(o(f("$gt", a(v("$test"), v("test"))))).apply(o(f("test", v("rest")))));
    assertEquals(
        v(true),
        function(o(f("$gt", a(v("$a.b"), v(false))))).apply(o(f("a", o(f("b", v(true)))))));
    assertEquals(
        v(false),
        function(o(f("$gt", a(v("$a.b"), v(true))))).apply(o(f("a", o(f("b", v(false)))))));
    assertEquals(
        v(false),
        function(o(f("$gt", a(v("$a.b"), v(true))))).apply(o(f("a", o(f("b", v(true)))))));
    assertEquals(
        v(false),
        function(o(f("$gt", a(v("$a.b"), v(false))))).apply(o(f("a", o(f("b", v(false)))))));
  }
}
