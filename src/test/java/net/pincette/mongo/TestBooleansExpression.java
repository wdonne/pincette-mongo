package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestBooleansExpression {
  @Test
  @DisplayName("$and")
  void and() {
    assertEquals(
        v(true),
        function(o(f("$and", a(v(true), v("$test"), v("test"))))).apply(o(f("test", v(true)))));
    assertEquals(
        v(false),
        function(o(f("$and", a(v(true), v("$test"), v("test"))))).apply(o(f("test", v(false)))));
    assertEquals(
        v(false),
        function(o(f("$and", a(v(true), v("$test"), v("test"))))).apply(o(f("test", v(null)))));
    assertEquals(
        v(false),
        function(o(f("$and", a(v(true), v("$test"), v(null))))).apply(o(f("test", v(true)))));
  }

  @Test
  @DisplayName("$not")
  void not() {
    assertEquals(v(true), function(o(f("$not", a(v("$test"))))).apply(o(f("test", v(false)))));
    assertEquals(v(true), function(o(f("$not", a(v("$test"))))).apply(o(f("test", v(0)))));
    assertEquals(v(true), function(o(f("$not", a(v("$test"))))).apply(o(f("test", v(null)))));
    assertEquals(v(false), function(o(f("$not", a(v("$test"))))).apply(o(f("test", v(true)))));
    assertEquals(v(false), function(o(f("$not", a(v("$test"))))).apply(o(f("test", v(1)))));
    assertEquals(v(false), function(o(f("$not", a(v("$test"))))).apply(o(f("test", o()))));
  }

  @Test
  @DisplayName("$or")
  void or() {
    assertEquals(
        v(true),
        function(o(f("$or", a(v(true), v("$test"), v("test"))))).apply(o(f("test", v(true)))));
    assertEquals(
        v(true),
        function(o(f("$or", a(v(true), v("$test"), v("test"))))).apply(o(f("test", v(false)))));
    assertEquals(
        v(false),
        function(o(f("$or", a(v(false), v("$test"), v(null))))).apply(o(f("test", v(null)))));
  }
}
