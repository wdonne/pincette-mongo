module net.pincette.mongo {
  requires java.json;
  requires net.pincette.json;
  requires java.logging;
  requires org.mongodb.bson;
  requires net.pincette.common;
  requires org.reactivestreams;
  requires net.pincette.rs;
  requires org.mongodb.driver.core;
  requires org.mongodb.driver.reactivestreams;
  requires com.schibsted.spt.data.jslt;
  requires net.thisptr.jackson.jq;

  exports net.pincette.mongo;
}
