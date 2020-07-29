package net.pincette.mongo;

import static java.lang.Math.E;
import static java.time.Instant.now;
import static net.pincette.json.Factory.a;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.mongo.Expression.function;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class TestArithmeticExpression {
  @Test
  @DisplayName("$abs")
  public void abs() {
    assertEquals(v(1), function(o(f("$abs", v(1)))).apply(o()));
    assertEquals(v(0), function(o(f("$abs", v(0)))).apply(o()));
    assertEquals(v(1), function(o(f("$abs", v(-1)))).apply(o()));
    assertNotEquals(v(-1), function(o(f("$abs", v(-1)))).apply(o()));
    assertEquals(v(1), function(o(f("$abs", v("$test")))).apply(o(f("test", v(1)))));
    assertNotEquals(v(-1), function(o(f("$abs", v("$test")))).apply(o(f("test", v(-1)))));
    assertEquals(
        v(2),
        function(o(f("$abs", o(f("$add", a(v("$test1"), v("$test2")))))))
            .apply(o(f("test1", v(1)), f("test2", v(1)))));
  }

  @Test
  @DisplayName("$add")
  public void add() {
    assertEquals(v(4), function(o(f("$add", a(v(1), v(3))))).apply(o()));
    assertEquals(v(9), function(o(f("$add", a(v(1), v(3), v(5))))).apply(o()));
    assertEquals(
        v("2020-04-21T00:00:03Z"),
        function(o(f("$add", a(v(1000), v("2020-04-21T00:00:00Z"), v(2000))))).apply(o()));
    assertNotEquals(v(8), function(o(f("$add", a(v(1), v(3), v(5))))).apply(o()));
    assertEquals(
        v(2),
        function(o(f("$add", a(v("$test.test1"), v("$test.test2")))))
            .apply(o(f("test", o(f("test1", v(1)), f("test2", v(1)))))));
    assertNotEquals(
        v(3),
        function(o(f("$add", a(v("$test.test1"), v("$test.test2")))))
            .apply(o(f("test", o(f("test1", v(1)), f("test2", v(1)))))));
  }

  @Test
  @DisplayName("$ceil")
  public void ceil() {
    assertEquals(v(1), function(o(f("$ceil", v(1)))).apply(o()));
    assertEquals(v(8), function(o(f("$ceil", v(7.5)))).apply(o()));
    assertEquals(v(-2), function(o(f("$ceil", v(-2.8)))).apply(o()));
    assertNotEquals(v(-2), function(o(f("$ceil", v(-1)))).apply(o()));
    assertEquals(v(1), function(o(f("$ceil", v("$test")))).apply(o(f("test", v(1)))));
    assertNotEquals(v(2), function(o(f("$ceil", v("$test")))).apply(o(f("test", v(1)))));
  }

  @Test
  @DisplayName("$divide")
  public void divide() {
    assertEquals(v(0.5), function(o(f("$divide", a(v(1), v(2))))).apply(o()));
    assertEquals(v(3), function(o(f("$divide", a(v(9), v(3))))).apply(o()));
    assertNotEquals(v(0), function(o(f("$divide", a(v(-1), v(2))))).apply(o()));
    assertEquals(
        v(3),
        function(o(f("$divide", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(9)), f("test2", v(3)))));
    assertNotEquals(
        v(4),
        function(o(f("$divide", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(9)), f("test2", v(3)))));
    assertEquals(
        v(0.3333333333333333),
        function(o(f("$divide", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(1)), f("test2", v(3)))));
    assertEquals(
        v(2.666666666666667),
        function(o(f("$divide", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(160)), f("test2", v(60)))));
  }

  @Test
  @DisplayName("$exp")
  public void exp() {
    assertEquals(v(1.0), function(o(f("$exp", v(0)))).apply(o()));
    assertEquals(v(7.38905609893065), function(o(f("$exp", v(2)))).apply(o()));
    assertNotEquals(v(2), function(o(f("$exp", v(0)))).apply(o()));
    assertEquals(v(1.0), function(o(f("$exp", v("$test")))).apply(o(f("test", v(0)))));
    assertNotEquals(v(0), function(o(f("$exp", v("$test")))).apply(o(f("test", v(0)))));
  }

  @Test
  @DisplayName("$floor")
  public void floor() {
    assertEquals(v(1), function(o(f("$floor", v(1)))).apply(o()));
    assertEquals(v(7), function(o(f("$floor", v(7.5)))).apply(o()));
    assertEquals(v(-3), function(o(f("$floor", v(-2.8)))).apply(o()));
    assertNotEquals(v(-2), function(o(f("$floor", v(-1)))).apply(o()));
    assertEquals(v(1), function(o(f("$floor", v("$test")))).apply(o(f("test", v(1)))));
    assertNotEquals(v(2), function(o(f("$floor", v("$test")))).apply(o(f("test", v(1)))));
  }

  @Test
  @DisplayName("$ln")
  public void ln() {
    assertEquals(v(0.0), function(o(f("$ln", v(1)))).apply(o()));
    assertEquals(v(1.0), function(o(f("$ln", v(E)))).apply(o()));
    assertEquals(v(2.302585092994046), function(o(f("$ln", v(10)))).apply(o()));
    assertNotEquals(v(0.0), function(o(f("$ln", v(E)))).apply(o()));
    assertEquals(v(0.0), function(o(f("$ln", v("$test")))).apply(o(f("test", v(1)))));
    assertNotEquals(v(1.0), function(o(f("$ln", v("$test")))).apply(o(f("test", v(1)))));
  }

  @Test
  @DisplayName("$log")
  public void log() {
    assertEquals(v(2.0), function(o(f("$log", a(v(100), v(10))))).apply(o()));
    assertEquals(v(4.605170185988092), function(o(f("$log", a(v(100), v(E))))).apply(o()));
    assertNotEquals(v(0.0), function(o(f("$log", a(v(100), v(10))))).apply(o()));
    assertEquals(
        v(2.0),
        function(o(f("$log", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(100)), f("test2", v(10)))));
    assertNotEquals(
        v(1.0),
        function(o(f("$log", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(100)), f("test2", v(10)))));
  }

  @Test
  @DisplayName("$log10")
  public void log10() {
    assertEquals(v(2.0), function(o(f("$log10", v(100)))).apply(o()));
    assertNotEquals(v(0.0), function(o(f("$log10", v(100)))).apply(o()));
    assertEquals(v(2.0), function(o(f("$log10", v("$test")))).apply(o(f("test", v(100)))));
    assertNotEquals(v(1.0), function(o(f("$log10", v("$test")))).apply(o(f("test", v(100)))));
  }

  @Test
  @DisplayName("$mod")
  public void mod() {
    assertEquals(v(0), function(o(f("$mod", a(v(100), v(10))))).apply(o()));
    assertEquals(v(1), function(o(f("$mod", a(v(5), v(2))))).apply(o()));
    assertNotEquals(v(1), function(o(f("$mod", a(v(100), v(10))))).apply(o()));
    assertEquals(
        v(0),
        function(o(f("$mod", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(100)), f("test2", v(10)))));
    assertNotEquals(
        v(0),
        function(o(f("$mod", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(5)), f("test2", v(2)))));
  }

  @Test
  @DisplayName("$multiply")
  public void multiply() {
    assertEquals(v(3), function(o(f("$multiply", a(v(1), v(3))))).apply(o()));
    assertEquals(v(15), function(o(f("$multiply", a(v(1), v(3), v(5))))).apply(o()));
    assertEquals(v(0.5), function(o(f("$multiply", a(v(0.5), v(1))))).apply(o()));
    assertNotEquals(v(8), function(o(f("$multiply", a(v(1), v(3), v(5))))).apply(o()));
    assertEquals(
        v(1.0),
        function(o(f("$multiply", a(v("$test.test1"), v("$test.test2")))))
            .apply(o(f("test", o(f("test1", v(2)), f("test2", v(0.5)))))));
    assertNotEquals(
        v(3),
        function(o(f("$multiply", a(v("$test.test1"), v("$test.test2")))))
            .apply(o(f("test", o(f("test1", v(1)), f("test2", v(1)))))));
  }

  @Test
  @DisplayName("$pow")
  public void pow() {
    assertEquals(v(1), function(o(f("$pow", a(v(5), v(0))))).apply(o()));
    assertEquals(v(25), function(o(f("$pow", a(v(5), v(2))))).apply(o()));
    assertEquals(v(0.04), function(o(f("$pow", a(v(5), v(-2))))).apply(o()));
    assertEquals(v(null), function(o(f("$pow", a(v(-5))))).apply(o()));
    assertEquals(v(1), function(o(f("$pow", a(v(5), v(0.1))))).apply(o()));
    assertEquals(v(0.2), function(o(f("$pow", a(v(5), v(-1))))).apply(o()));
    assertNotEquals(v(1), function(o(f("$pow", a(v(100), v(10))))).apply(o()));
    assertEquals(
        v(125),
        function(o(f("$pow", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(5)), f("test2", v(3)))));
    assertNotEquals(
        v(24),
        function(o(f("$pow", a(v("$test1"), v("$test2")))))
            .apply(o(f("test1", v(5)), f("test2", v(2)))));
  }

  @Test
  @DisplayName("$round")
  public void round() {
    assertEquals(v(6), function(o(f("$round", a(v(5.5))))).apply(o()));
    assertEquals(v(5), function(o(f("$round", a(v(5.1))))).apply(o()));
    assertEquals(v(1555.11), function(o(f("$round", a(v(1555.1111), v(2))))).apply(o()));
    assertEquals(v(1600), function(o(f("$round", a(v(1555.1111), v(-2))))).apply(o()));
    assertEquals(v(1500), function(o(f("$round", a(v(1515.1111), v(-2))))).apply(o()));
    assertEquals(v(1555), function(o(f("$round", a(v(1555.1111), v(0))))).apply(o()));
    assertNotEquals(v(1500), function(o(f("$round", a(v(1555.1111), v(0))))).apply(o()));
  }

  @Test
  @DisplayName("$sqrt")
  public void sqrt() {
    assertEquals(v(5.0), function(o(f("$sqrt", v(25)))).apply(o()));
    assertEquals(v(5.477225575051661), function(o(f("$sqrt", v(30)))).apply(o()));
    assertEquals(v(null), function(o(f("$sqrt", v(null)))).apply(o()));
  }

  @Test
  @DisplayName("$subtract")
  public void subtract() {
    assertEquals(v(6), function(o(f("$subtract", a(v(12), v(6))))).apply(o()));
    assertNotEquals(v(5), function(o(f("$subtract", a(v(12), v(6))))).apply(o()));
    assertEquals(
        v("2020-04-21T00:00:29Z"),
        function(o(f("$subtract", a(v("2020-04-21T00:00:30Z"), v(1000))))).apply(o()));
    assertEquals(
        v(1000),
        function(o(f("$subtract", a(v("2020-04-21T00:00:30Z"), v("2020-04-21T00:00:29Z")))))
            .apply(o()));
    assertNotEquals(
        v(now().minusMillis(5000).toString()),
        function(o(f("$subtract", a(v("$$NOW"), v(1000))))).apply(o()));
  }

  @Test
  @DisplayName("$trunc")
  public void trunc() {
    assertEquals(v(5), function(o(f("$trunc", a(v(5.5))))).apply(o()));
    assertEquals(v(5), function(o(f("$trunc", a(v(5.1))))).apply(o()));
    assertEquals(v(1555.11), function(o(f("$trunc", a(v(1555.1111), v(2))))).apply(o()));
    assertEquals(v(1555.11), function(o(f("$trunc", a(v(1555.1151), v(2))))).apply(o()));
    assertEquals(v(1500), function(o(f("$trunc", a(v(1555.1111), v(-2))))).apply(o()));
    assertEquals(v(1500), function(o(f("$trunc", a(v(1515.1111), v(-2))))).apply(o()));
    assertEquals(v(1555), function(o(f("$trunc", a(v(1555.1111), v(0))))).apply(o()));
    assertEquals(v(1555), function(o(f("$trunc", a(v(1555.1151), v(0))))).apply(o()));
    assertNotEquals(v(1500), function(o(f("$trunc", a(v(1555.1111), v(0))))).apply(o()));
  }
}
