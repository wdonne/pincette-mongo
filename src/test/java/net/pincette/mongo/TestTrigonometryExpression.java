package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestTrigonometryExpression {
  @Test
  @DisplayName("$acos")
  void acos() {
    assertEquals(
        v(60),
        function(o(f("$round", a(o(f("$radiansToDegrees", o(f("$acos", v(0.5))))), v(1)))))
            .apply(o()));
    assertEquals(v(null), function(o(f("$acos", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$acosh")
  void acosh() {
    assertEquals(
        v(101),
        function(o(f("$round", a(o(f("$radiansToDegrees", o(f("$acosh", v(3))))), v(1)))))
            .apply(o()));
    assertEquals(v(null), function(o(f("$acosh", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$asin")
  void asin() {
    assertEquals(v(90.0), function(o(f("$radiansToDegrees", o(f("$asin", v(1)))))).apply(o()));
    assertEquals(v(null), function(o(f("$asin", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$asinh")
  void asinh() {
    assertEquals(
        v(50.5),
        function(o(f("$round", a(o(f("$radiansToDegrees", o(f("$asinh", v(1))))), v(1)))))
            .apply(o()));
    assertEquals(v(null), function(o(f("$asinh", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$atan")
  void atan() {
    assertEquals(
        v(45),
        function(o(f("$round", a(o(f("$radiansToDegrees", o(f("$atan", v(1))))), v(1)))))
            .apply(o()));
    assertEquals(v(null), function(o(f("$atan", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$atan2")
  void atan2() {
    assertEquals(
        v(53.1),
        function(o(f("$round", a(o(f("$radiansToDegrees", o(f("$atan2", a(v(4), v(3)))))), v(1)))))
            .apply(o()));
    assertEquals(v(null), function(o(f("$atan2", a(v(null), v(null))))).apply(o()));
  }

  @Test
  @DisplayName("$atanh")
  void atanh() {
    assertEquals(
        v(31.5),
        function(o(f("$round", a(o(f("$radiansToDegrees", o(f("$atanh", v(0.5))))), v(1)))))
            .apply(o()));
    assertEquals(v(null), function(o(f("$atanh", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$cos")
  void cos() {
    assertEquals(
        v(0.5),
        function(o(f("$round", a(o(f("$cos", o(f("$degreesToRadians", v(60))))), v(1)))))
            .apply(o()));
    assertEquals(v(null), function(o(f("$cos", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$sin")
  void sin() {
    assertEquals(v(1.0), function(o(f("$sin", o(f("$degreesToRadians", v(90)))))).apply(o()));
    assertEquals(v(null), function(o(f("$sin", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$tan")
  void tan() {
    assertEquals(
        v(1),
        function(o(f("$round", a(o(f("$tan", o(f("$degreesToRadians", v(45))))), v(1)))))
            .apply(o()));
    assertEquals(v(null), function(o(f("$tan", v(null)))).apply(o()));
  }
}
