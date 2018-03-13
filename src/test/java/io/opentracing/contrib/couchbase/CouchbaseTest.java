/*
 * Copyright 2018 The OpenTracing Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.opentracing.contrib.couchbase;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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
import io.opentracing.Span;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.rxjava.TracingActionSubscriber;
import io.opentracing.rxjava.TracingObserverSubscriber;
import io.opentracing.rxjava.TracingRxJavaUtils;
import io.opentracing.rxjava.TracingSubscriber;
import io.opentracing.tag.Tags;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import rx.Completable.OnSubscribe;
import rx.Observer;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func1;
import rx.plugins.RxJavaHooks;

@Ignore
public class CouchbaseTest {

  private MockTracer mockTracer = new MockTracer();
  private Cluster cluster;
  private ClusterManager clusterManager;
  private final String bucketName = "default";

  @Before
  public void before() {
    cluster = new TracingCluster(CouchbaseCluster.create("localhost"), mockTracer, false);
    String username = "Administrator";
    String password = "password";
    cluster.authenticate(username, password);
    clusterManager = cluster.clusterManager(username, password);
  }

  @After
  public void after() {
    cluster.disconnect();
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

    Assert.assertNull(mockTracer.activeSpan());

    int spansCount = 5;
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

  @Test
  public void asyncAction() {
    final Bucket bucket = cluster.openBucket(bucketName);

    TracingRxJavaUtils.enableTracing(mockTracer);

    RxJavaHooks.setOnCompletableCreate(new Func1<OnSubscribe, OnSubscribe>() {
      @Override
      public OnSubscribe call(OnSubscribe onSubscribe) {
        return onSubscribe;
      }
    });

    Action1<JsonDocument> onNext = new Action1<JsonDocument>() {
      @Override
      public void call(JsonDocument document) {
        Span span = mockTracer.activeSpan();
        System.out.println(document.id());
      }
    };

    Action1<Throwable> onError = new Action1<Throwable>() {
      @Override
      public void call(Throwable throwable) {
        Span span = mockTracer.activeSpan();
        System.out.println(throwable.toString());
      }
    };

    Action0 onCompleted = new Action0() {
      @Override
      public void call() {
        Span span = mockTracer.activeSpan();
        System.out.println("onCompleted");
      }
    };

    final TracingActionSubscriber<JsonDocument> tracingActionSubscriber = new TracingActionSubscriber<>(
        onNext, onError, onCompleted, "get", "java-couchbase", mockTracer);


    bucket
        .async()
        .get("u:king_arthur")
        .map(new Func1<JsonDocument, JsonDocument>() {
          @Override
          public JsonDocument call(JsonDocument document) {
            Span span = mockTracer.activeSpan();
            Span span1 = tracingActionSubscriber.getSpan();

            return document;
          }
        })
        .subscribe(tracingActionSubscriber);

    await().atMost(15, TimeUnit.SECONDS).until(reportedSpansSize(), equalTo(2));

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());

    assertNull(mockTracer.activeSpan());


  }

  @Test
  public void asyncObserver() {
    final Bucket bucket = cluster.openBucket(bucketName);

    TracingRxJavaUtils.enableTracing(mockTracer);

    Observer<JsonDocument> observer = new Observer<JsonDocument>() {
      @Override
      public void onCompleted() {

        Span span = mockTracer.activeSpan();
        System.out.println("completed");
      }

      @Override
      public void onError(Throwable e) {

      }

      @Override
      public void onNext(JsonDocument document) {
        Span span = mockTracer.activeSpan();
        System.out.println(document.id());
      }
    };

    TracingObserverSubscriber<JsonDocument> tracingObserverSubscriber = new TracingObserverSubscriber<>(
        observer, "get", mockTracer);

    bucket
        .async()
        .get("u:king_arthur")
        .map(new Func1<JsonDocument, JsonDocument>() {
          @Override
          public JsonDocument call(JsonDocument document) {
            Span span = mockTracer.activeSpan();
            return document;
          }
        })
        .subscribe(tracingObserverSubscriber);

    await().atMost(15, TimeUnit.SECONDS).until(reportedSpansSize(), equalTo(2));

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());

    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void testAsyncSubscriber() {
    final Bucket bucket = cluster.openBucket(bucketName);

    TracingRxJavaUtils.enableTracing(mockTracer);

    Subscriber<JsonDocument> subscriber = new Subscriber<JsonDocument>() {
      @Override
      public void onCompleted() {
        Span span = mockTracer.activeSpan();
        System.out.println("onCompleted");
      }

      @Override
      public void onError(Throwable e) {
        Span span = mockTracer.activeSpan();
        e.printStackTrace();
      }

      @Override
      public void onNext(JsonDocument document) {
        Span span = mockTracer.activeSpan();
        System.out.println(document.id());
      }
    };

    TracingSubscriber<JsonDocument> tracingSubscriber = new TracingSubscriber<>(subscriber, "get",
        mockTracer);

    bucket
        .async()
        .get("u:king_arthur")
        .map(new Func1<JsonDocument, JsonDocument>() {
          @Override
          public JsonDocument call(JsonDocument document) {
            Span span = mockTracer.activeSpan();
            return document;
          }
        })
        .subscribe(tracingSubscriber);

    await().atMost(15, TimeUnit.SECONDS).until(reportedSpansSize(), equalTo(2));

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(2, spans.size());

    assertNull(mockTracer.activeSpan());
  }

  private Callable<Integer> reportedSpansSize() {
    return new Callable<Integer>() {
      @Override
      public Integer call() {
        return mockTracer.finishedSpans().size();
      }
    };
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



// TODO: async with parent
