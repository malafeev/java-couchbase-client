package io.opentracing.contrib.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import io.opentracing.mock.MockTracer;
import io.opentracing.util.ThreadLocalScopeManager;
import org.junit.Test;

public class CouchbaseTest {
  private MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(),
      MockTracer.Propagator.TEXT_MAP);

  @Test
  public void test() {
// Initialize the Connection
    Cluster cluster = new TracingCluster(CouchbaseCluster.create("localhost"), mockTracer, false);
    cluster.authenticate("Administrator", "password");

    ClusterManager clusterManager = cluster.clusterManager("Administrator", "password");
    if (clusterManager.getBucket("default") == null) {

      BucketSettings bucketSettings = new DefaultBucketSettings.Builder()
          .type(BucketType.COUCHBASE)
          .name("default")
          .quota(120)
          .build();

      clusterManager.insertBucket(bucketSettings);
    }

    Bucket bucket = cluster.openBucket("default");


    // Create a JSON Document
    JsonObject arthur = JsonObject.create()
        .put("name", "Arthur")
        .put("email", "kingarthur@couchbase.com")
        .put("interests", JsonArray.from("Holy Grail", "African Swallows"));

    // Store the Document
    bucket.upsert(JsonDocument.create("u:king_arthur", arthur));

    // Load the Document and print it
    // Prints Content and Metadata of the stored Document
    System.out.println(bucket.get("u:king_arthur"));

    // Create a N1QL Primary Index (but ignore if it exists)
    bucket.bucketManager().createN1qlPrimaryIndex(true, false);

    // Perform a N1QL Query
    N1qlQueryResult result = bucket.query(
        N1qlQuery.parameterized("SELECT name FROM bucketname WHERE $1 IN interests",
            JsonArray.from("African Swallows"))
    );

    // Print each found Row
    for (N1qlQueryRow row : result) {
      // Prints {"name":"Arthur"}
      System.out.println(row);
    }

    cluster.disconnect();

    System.out.println(mockTracer.finishedSpans().size());
  }

}
