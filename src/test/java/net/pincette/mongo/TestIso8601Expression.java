package net.pincette.mongo;

import static java.time.Instant.now;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static net.pincette.json.Factory.f;
import static net.pincette.json.Factory.o;
import static net.pincette.json.Factory.v;
import static net.pincette.json.JsonUtil.asInstant;
import static net.pincette.mongo.Expression.function;
import static net.pincette.mongo.Iso8601.fromInstant;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestIso8601Expression {
  private static long toNanos(final Instant instant) {
    return instant.getEpochSecond() * 1000000000 + instant.getNano();
  }

  @Test
  @DisplayName("fromEpochMillis")
  void fromEpochMillis() {
    final Instant n = now();

    assertEquals(
        n.toEpochMilli(),
        asInstant(function(o(f("$fromEpochMillis", v(n.toEpochMilli())))).apply(o()))
            .toEpochMilli());
  }

  @Test
  @DisplayName("fromEpochNanos")
  void fromEpochNanos() {
    final long l = toNanos(now());

    assertEquals(l, toNanos(asInstant(function(o(f("$fromEpochNanos", v(l)))).apply(o()))));
  }

  @Test
  @DisplayName("fromEpochSeconds")
  void fromEpochSeconds() {
    final Instant n = now();

    assertEquals(
        n.getEpochSecond(),
        asInstant(function(o(f("$fromEpochSeconds", v(n.getEpochSecond())))).apply(o()))
            .getEpochSecond());
  }

  @Test
  @DisplayName("toDate")
  void toDate() {
    final Instant n = now();

    assertEquals(
        v(fromInstant(n).format(ISO_DATE)), function(o(f("$toDate", v(n.toString())))).apply(o()));
  }

  @Test
  @DisplayName("toDay")
  void toDay() {
    final Instant n = now();

    assertEquals(
        v(fromInstant(n).getDayOfMonth()), function(o(f("$toDay", v(n.toString())))).apply(o()));
  }

  @Test
  @DisplayName("toEpochMillis")
  void toEpochMillis() {
    final Instant n = now();

    assertEquals(v(n.toEpochMilli()), function(o(f("$toEpochMillis", v(n.toString())))).apply(o()));
  }

  @Test
  @DisplayName("toEpochNanos")
  void toEpochNanos() {
    final Instant n = now();

    assertEquals(v(toNanos(n)), function(o(f("$toEpochNanos", v(n.toString())))).apply(o()));
  }

  @Test
  @DisplayName("toEpochSeconds")
  void toEpochSeconds() {
    final Instant n = now();

    assertEquals(
        v(n.getEpochSecond()), function(o(f("$toEpochSeconds", v(n.toString())))).apply(o()));
  }

  @Test
  @DisplayName("toMonth")
  void toMonth() {
    final Instant n = now();

    assertEquals(
        v(fromInstant(n).getMonthValue()), function(o(f("$toMonth", v(n.toString())))).apply(o()));
  }

  @Test
  @DisplayName("toYear")
  void toYear() {
    final Instant n = now();

    assertEquals(
        v(fromInstant(n).getYear()), function(o(f("$toYear", v(n.toString())))).apply(o()));
  }
}
