package io.opentracing.contrib.couchbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.ThreadLocalScopeManager;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CouchbaseTest {

  private MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(),
      MockTracer.Propagator.TEXT_MAP);
  private Cluster cluster;
  private ClusterManager clusterManager;
  private final String bucketName = "default";

  @Before
  public void before() {
    cluster = new TracingCluster(CouchbaseCluster.create("localhost"), mockTracer, false);
    cluster.authenticate("Administrator", "password");
    clusterManager = cluster.clusterManager("Administrator", "password");
  }

  @Test
  public void test() {
    boolean bucketCreated = createBucketIfMissing();

    Bucket bucket = cluster.openBucket(bucketName);

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
        N1qlQuery.parameterized("SELECT name FROM default WHERE $1 IN interests",
            JsonArray.from("African Swallows"))
    );

    // Print each found Row
    for (N1qlQueryRow row : result) {
      // Prints {"name":"Arthur"}
      System.out.println(row);
    }

    cluster.disconnect();

    Assert.assertNull(mockTracer.activeSpan());

    int spansCount = 6;
    if (bucketCreated) {
      spansCount += 1;
    }

    List<MockSpan> spans = mockTracer.finishedSpans();
    Assert.assertEquals(spansCount, spans.size());
    for (MockSpan span : spans) {
      assertTrue(span.tags().get(Tags.SPAN_KIND.getKey()).equals(Tags.SPAN_KIND_CLIENT));
      assertEquals(TracingHelper.COMPONENT_NAME,
          span.tags().get(Tags.COMPONENT.getKey()));
      assertEquals(0, span.generatedErrors().size());
      assertEquals(0, span.parentId());
    }
  }

  private boolean createBucketIfMissing() {
    if (clusterManager.getBucket(bucketName) == null) {

      BucketSettings bucketSettings = new DefaultBucketSettings.Builder()
          .type(BucketType.COUCHBASE)
          .name(bucketName)
          .quota(120)
          .build();

      clusterManager.insertBucket(bucketSettings);
      return true;
    }
    return false;
  }

}
