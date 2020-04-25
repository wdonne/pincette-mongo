package net.pincette.mongo;

import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TestCmpExpression {
  @Test
  @DisplayName("$cmp")
  public void cmp() {
    assertEquals(
        v(-1),
        function(o(f("$cmp", a(v("$test1.v"), v("$test2.v")))))
            .apply(o(f("test1", o(f("v", v(0)))), f("test2", o(f("v", v(1)))))));
    assertEquals(
        v(1),
        function(o(f("$cmp", a(v("$test1.v"), v("$test2.v")))))
            .apply(o(f("test1", o(f("v", v(1)))), f("test2", o(f("v", v(0)))))));
    assertEquals(
        v(0),
        function(o(f("$cmp", a(v("$test1.v"), v("$test2.v")))))
            .apply(o(f("test1", o(f("v", v(0)))), f("test2", o(f("v", v(0)))))));
    assertEquals(
        v(-1),
        function(o(f("$cmp", a(v("$test1.v"), v("$test2.v")))))
            .apply(o(f("test1", o(f("v", v(0)))), f("test2", o(f("v", o()))))));
    assertEquals(
        v(-1),
        function(o(f("$cmp", a(v("$test1.v"), v("$test2.v")))))
            .apply(o(f("test1", o(f("v", v(0)))), f("test2", o(f("v", v("a")))))));
  }
}
