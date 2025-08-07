package net.pincette.mongo;

import static java.time.Instant.ofEpochMilli;
import static java.time.LocalDate.ofInstant;
import static java.time.ZoneId.systemDefault;
import static java.time.format.DateTimeFormatter.ISO_DATE;
import static javax.json.JsonValue.NULL;
import static net.pincette.mongo.Expression.asInstant;
import static net.pincette.mongo.Expression.asLong;
import static net.pincette.mongo.Expression.implementation;

import java.time.Instant;
import java.time.LocalDate;
import java.util.function.Function;
import javax.json.JsonValue;
import net.pincette.json.JsonUtil;

class Iso8601 {
  private Iso8601() {}

  private static Implementation fromEpoch(
      final JsonValue value, final Function<Long, Instant> fn, final Features features) {
    final ImplementationOptional implementation = asLong(implementation(value, features));

    return (json, vars) ->
        implementation
            .apply(json, vars)
            .map(JsonUtil::asLong)
            .map(fn)
            .map(JsonUtil::createValue)
            .orElse(NULL);
  }

  static Implementation fromEpochMillis(final JsonValue value, final Features features) {
    return fromEpoch(value, Instant::ofEpochMilli, features);
  }

  static Implementation fromEpochNanos(final JsonValue value, final Features features) {
    return fromEpoch(
        value, l -> ofEpochMilli(l / 1000000).plusNanos(l - (l / 1000000) * 1000000), features);
  }

  static Implementation fromEpochSeconds(final JsonValue value, final Features features) {
    return fromEpoch(value, Instant::ofEpochSecond, features);
  }

  static LocalDate fromInstant(final Instant instant) {
    return ofInstant(instant, systemDefault());
  }

  static Implementation toDate(final JsonValue value, final Features features) {
    return toValue(value, i -> fromInstant(i).format(ISO_DATE), features);
  }

  static Implementation toDay(final JsonValue value, final Features features) {
    return toValue(value, i -> fromInstant(i).getDayOfMonth(), features);
  }

  static Implementation toEpochMillis(final JsonValue value, final Features features) {
    return toValue(value, Instant::toEpochMilli, features);
  }

  static Implementation toEpochNanos(final JsonValue value, final Features features) {
    return toValue(value, i -> i.getEpochSecond() * 1000000000 + i.getNano(), features);
  }

  static Implementation toEpochSeconds(final JsonValue value, final Features features) {
    return toValue(value, Instant::getEpochSecond, features);
  }

  static Implementation toMonth(final JsonValue value, final Features features) {
    return toValue(value, i -> fromInstant(i).getMonthValue(), features);
  }

  private static <T> Implementation toValue(
      final JsonValue value, final Function<Instant, T> fn, final Features features) {
    final ImplementationOptional implementation = asInstant(implementation(value, features));

    return (json, vars) ->
        implementation
            .apply(json, vars)
            .map(JsonUtil::asInstant)
            .map(fn)
            .map(JsonUtil::createValue)
            .orElse(NULL);
  }

  static Implementation toYear(final JsonValue value, final Features features) {
    return toValue(value, i -> fromInstant(i).getYear(), features);
  }
}
