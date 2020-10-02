package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestTypesExpression {
  @Test
  @DisplayName("$convert")
  void convert() {
    assertEquals(
        v(true), function(o(f("$convert", o(f("input", v(1)), f("to", v("bool")))))).apply(o()));
    assertEquals(
        v(true), function(o(f("$convert", o(f("input", v(1)), f("to", v(8)))))).apply(o()));
    assertEquals(
        v(null), function(o(f("$convert", o(f("input", v(null)), f("to", v(8)))))).apply(o()));
    assertEquals(
        v(false),
        function(o(f("$convert", o(f("input", v(null)), f("to", v(8)), f("onNull", v(false))))))
            .apply(o()));
    assertEquals(
        v(null),
        function(
                o(f("$convert", o(f("input", o()), f("to", v("decimal")), f("onError", v(false))))))
            .apply(o()));
  }

  @Test
  @DisplayName("$toBool")
  void toBool() {
    assertEquals(v(true), function(o(f("$toBool", v(true)))).apply(o()));
    assertEquals(v(true), function(o(f("$toBool", v(1)))).apply(o()));
    assertEquals(v(true), function(o(f("$toBool", v("a")))).apply(o()));
    assertEquals(v(false), function(o(f("$toBool", v(false)))).apply(o()));
    assertEquals(v(false), function(o(f("$toBool", v(0)))).apply(o()));
    assertEquals(v(null), function(o(f("$toBool", v(null)))).apply(o()));
    assertEquals(v(null), function(o(f("$toBool", o()))).apply(o()));
    assertEquals(v(null), function(o(f("$toBool", a()))).apply(o()));
  }

  @Test
  @DisplayName("$toDecimal")
  void toDecimal() {
    assertEquals(v(1), function(o(f("$toDecimal", v(1)))).apply(o()));
    assertEquals(v(1.0), function(o(f("$toDecimal", v(1.0)))).apply(o()));
    assertEquals(v(1.0), function(o(f("$toDecimal", v("1.0")))).apply(o()));
    assertEquals(v(1), function(o(f("$toDecimal", v(true)))).apply(o()));
    assertEquals(v(0), function(o(f("$toDecimal", v(false)))).apply(o()));
    assertEquals(v(null), function(o(f("$toDecimal", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$toDouble")
  void toDouble() {
    assertEquals(v(1), function(o(f("$toDouble", v(1)))).apply(o()));
    assertEquals(v(1.0), function(o(f("$toDouble", v(1.0)))).apply(o()));
    assertEquals(v(1.0), function(o(f("$toDouble", v("1.0")))).apply(o()));
    assertEquals(v(1), function(o(f("$toDouble", v(true)))).apply(o()));
    assertEquals(v(0), function(o(f("$toDouble", v(false)))).apply(o()));
    assertEquals(v(null), function(o(f("$toDouble", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$toInt")
  void toInt() {
    assertEquals(v(1), function(o(f("$toInt", v(1)))).apply(o()));
    assertEquals(v(1), function(o(f("$toInt", v(1.0)))).apply(o()));
    assertEquals(v(1), function(o(f("$toInt", v("1.0")))).apply(o()));
    assertEquals(v(1), function(o(f("$toInt", v(true)))).apply(o()));
    assertEquals(v(0), function(o(f("$toInt", v(false)))).apply(o()));
    assertEquals(v(null), function(o(f("$toInt", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$toLong")
  void toLong() {
    assertEquals(v(1), function(o(f("$toLong", v(1)))).apply(o()));
    assertEquals(v(1), function(o(f("$toLong", v(1.0)))).apply(o()));
    assertEquals(v(1), function(o(f("$toLong", v("1.0")))).apply(o()));
    assertEquals(v(1), function(o(f("$toLong", v(true)))).apply(o()));
    assertEquals(v(0), function(o(f("$toLong", v(false)))).apply(o()));
    assertEquals(v(null), function(o(f("$toLong", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$toString")
  void toStringTest() {
    assertEquals(v("1"), function(o(f("$toString", v(1)))).apply(o()));
    assertEquals(v("1.0"), function(o(f("$toString", v(1.0)))).apply(o()));
    assertEquals(v("1.0"), function(o(f("$toString", v("1.0")))).apply(o()));
    assertEquals(v("true"), function(o(f("$toString", v(true)))).apply(o()));
    assertEquals(v("false"), function(o(f("$toString", v(false)))).apply(o()));
    assertEquals(v(null), function(o(f("$toString", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$type")
  void type() {
    assertEquals(v("bool"), function(o(f("$type", v(true)))).apply(o()));
    assertEquals(v("bool"), function(o(f("$type", v(false)))).apply(o()));
    assertEquals(v("string"), function(o(f("$type", v("a")))).apply(o()));
    assertEquals(v("int"), function(o(f("$type", v(1)))).apply(o()));
    assertEquals(v("long"), function(o(f("$type", v(1000000000000000L)))).apply(o()));
    assertEquals(v("double"), function(o(f("$type", v(1.1)))).apply(o()));
    assertEquals(v("object"), function(o(f("$type", v("$test")))).apply(o(f("test", o()))));
    assertEquals(v("array"), function(o(f("$type", v("$test")))).apply(o(f("test", a()))));
  }
}
