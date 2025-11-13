package net.pincette.mongo;

import static java.time.Instant.now;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import javax.json.JsonValue;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class TestBsonUtil {
  private static JsonValue isoDate(final Instant instant) {
    return createValue("ISODate(\"" + instant + "\")");
  }

  @Test
  @DisplayName("fromJson 1")
  void fromJson1() {
    final Instant i = now();

    assertEquals(new BsonDateTime(i.toEpochMilli()), BsonUtil.fromJson(isoDate(i)));
  }

  @Test
  @DisplayName("fromJson 2")
  void fromJson2() {
    final BsonDocument doc = new BsonDocument();
    final Instant i = now();

    doc.put("test", new BsonDateTime(i.toEpochMilli()));
    assertEquals(doc, BsonUtil.fromJson(createObjectBuilder().add("test", isoDate(i)).build()));
  }
}
