package net.pincette.mongo;

import static java.time.Instant.ofEpochMilli;
import static java.time.Instant.ofEpochSecond;
import static java.time.Instant.parse;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static javax.json.JsonValue.FALSE;
import static javax.json.JsonValue.NULL;
import static javax.json.JsonValue.TRUE;
import static net.pincette.json.JsonUtil.asNumber;
import static net.pincette.json.JsonUtil.asString;
import static net.pincette.json.JsonUtil.createArrayBuilder;
import static net.pincette.json.JsonUtil.createObjectBuilder;
import static net.pincette.json.JsonUtil.createValue;
import static net.pincette.json.JsonUtil.string;
import static net.pincette.json.JsonUtil.stringValue;
import static net.pincette.util.Util.tryToGetSilent;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import org.bson.BsonArray;
import org.bson.BsonBinaryWriter;
import org.bson.BsonBoolean;
import org.bson.BsonDateTime;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonNumber;
import org.bson.BsonObjectId;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.BsonValueCodecProvider;
import org.bson.codecs.DocumentCodecProvider;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.IterableCodecProvider;
import org.bson.codecs.MapCodecProvider;
import org.bson.codecs.ValueCodecProvider;
import org.bson.conversions.Bson;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.OutputBuffer;
import org.bson.types.ObjectId;

/**
 * BSON utilities.
 *
 * @author Werner Donn\u00e9
 * @since 1.0
 */
public class BsonUtil {
  private static final String ID = "_id";

  private BsonUtil() {}

  public static JsonValue fromBson(final BsonValue bson) {
    switch (bson.getBsonType()) {
      case ARRAY:
        return fromBson(bson.asArray());
      case BOOLEAN:
        return bson.asBoolean().getValue() ? TRUE : FALSE;
      case DATE_TIME:
        return fromBson(bson.asDateTime());
      case DOCUMENT:
        return fromBson(bson.asDocument());
      case DOUBLE:
        return fromBson(bson.asDouble());
      case INT32:
        return fromBson(bson.asInt32());
      case INT64:
        return fromBson(bson.asInt64());
      case NULL:
        return NULL;
      case OBJECT_ID:
        return fromBson(bson.asObjectId());
      case REGULAR_EXPRESSION:
        return fromBson(bson.asRegularExpression());
      case STRING:
        return fromBson(bson.asString());
      case TIMESTAMP:
        return fromBson(bson.asTimestamp());
      default:
        return null;
    }
  }

  public static JsonArray fromBson(final BsonArray array) {
    return array.stream()
        .map(BsonUtil::fromBson)
        .reduce(createArrayBuilder(), JsonArrayBuilder::add, (b1, b2) -> b1)
        .build();
  }

  public static JsonObject fromBson(final BsonDocument bson) {
    return bson.entrySet().stream()
        .reduce(
            createObjectBuilder(),
            (b, e) -> b.add(e.getKey(), fromBson(e.getValue())),
            (b1, b2) -> b1)
        .build();
  }

  public static JsonNumber fromBson(final BsonInt32 bson) {
    return asNumber(createValue(bson.getValue()));
  }

  public static JsonNumber fromBson(final BsonInt64 bson) {
    return asNumber(createValue(bson.getValue()));
  }

  public static JsonNumber fromBson(final BsonDouble bson) {
    return asNumber(createValue(bson.getValue()));
  }

  public static JsonString fromBson(final BsonDateTime bson) {
    return asString(createValue(ofEpochMilli(bson.getValue()).toString()));
  }

  public static JsonString fromBson(final BsonObjectId bson) {
    return asString(createValue(bson.getValue().toHexString()));
  }

  public static JsonObject fromBson(final BsonRegularExpression bson) {
    return createObjectBuilder()
        .add("$regex", bson.getPattern())
        .add("$options", bson.getOptions())
        .build();
  }

  public static JsonString fromBson(final BsonString bson) {
    return asString(createValue(bson.getValue()));
  }

  public static JsonString fromBson(final BsonTimestamp bson) {
    return asString(createValue(ofEpochSecond(bson.getTime()).toString()));
  }

  public static BsonValue fromJson(final JsonValue json) {
    switch (json.getValueType()) {
      case ARRAY:
        return fromJson(json.asJsonArray());
      case FALSE:
        return BsonBoolean.FALSE;
      case NULL:
        return BsonNull.VALUE;
      case NUMBER:
        return fromJson(asNumber(json));
      case OBJECT:
        return fromJson(json.asJsonObject());
      case STRING:
        return fromJson(asString(json));
      case TRUE:
        return BsonBoolean.TRUE;
      default:
        return null;
    }
  }

  public static BsonArray fromJson(final JsonArray array) {
    return new BsonArray(array.stream().map(BsonUtil::fromJson).collect(toList()));
  }

  public static BsonDocument fromJson(final JsonObject json) {
    return json.entrySet().stream()
        .reduce(
            new BsonDocument(),
            (d, e) ->
                d.append(
                    e.getKey(),
                    isObjectId(e.getKey(), e.getValue())
                        ? fromJsonObjectId(e.getValue())
                        : fromJson(e.getValue())),
            (d1, d2) -> d1);
  }

  public static BsonNumber fromJson(final JsonNumber json) {
    return json.isIntegral() ? new BsonInt64(json.longValue()) : new BsonDouble(json.doubleValue());
  }

  public static BsonValue fromJson(final JsonString json) {
    return Optional.of(json.getString())
        .flatMap(s -> tryToGetSilent(() -> parse(s)))
        .map(Instant::toEpochMilli)
        .map(BsonDateTime::new)
        .map(BsonValue.class::cast)
        .orElseGet(() -> new BsonString(json.getString()));
  }

  private static BsonValue fromJsonObjectId(final JsonValue json) {
    return new BsonObjectId(new ObjectId(string(json)));
  }

  private static boolean isHex(final String s) {
    for (int i = 0; i < s.length(); ++i) {
      if (!isHex(s.charAt(i))) {
        return false;
      }
    }

    return true;
  }

  private static boolean isHex(final char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
  }

  private static boolean isObjectId(final String key, final JsonValue value) {
    return key.equals(ID)
        && stringValue(value).filter(s -> s.length() == 24).filter(BsonUtil::isHex).isPresent();
  }

  public static BsonDocument toBsonDocument(final Bson bson) {
    return bson.toBsonDocument(
        BsonDocument.class,
        fromProviders(
            new BsonValueCodecProvider(),
            new ValueCodecProvider(),
            new DocumentCodecProvider(),
            new IterableCodecProvider(),
            new MapCodecProvider()));
  }

  public static byte[] toBytes(final BsonDocument document) {
    final OutputBuffer out = new BasicOutputBuffer();

    new BsonDocumentCodec()
        .encode(new BsonBinaryWriter(out), document, EncoderContext.builder().build());

    return out.toByteArray();
  }

  public static Document toDocument(final BsonDocument document) {
    return new Document(
        document.entrySet().stream().collect(toMap(Map.Entry::getKey, Map.Entry::getValue)));
  }
}
