package io.opentracing.contrib.couchbase;

import static io.opentracing.contrib.couchbase.TracingHelper.nullable;

import com.couchbase.client.core.ClusterFacade;
import com.couchbase.client.core.annotations.InterfaceAudience.Public;
import com.couchbase.client.core.annotations.InterfaceStability.Committed;
import com.couchbase.client.core.annotations.InterfaceStability.Experimental;
import com.couchbase.client.core.annotations.InterfaceStability.Uncommitted;
import com.couchbase.client.core.message.internal.PingReport;
import com.couchbase.client.core.service.ServiceType;
import com.couchbase.client.java.AsyncBucket;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.PersistTo;
import com.couchbase.client.java.ReplicaMode;
import com.couchbase.client.java.ReplicateTo;
import com.couchbase.client.java.analytics.AnalyticsQuery;
import com.couchbase.client.java.analytics.AnalyticsQueryResult;
import com.couchbase.client.java.bucket.BucketManager;
import com.couchbase.client.java.datastructures.MutationOptionBuilder;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.JsonLongDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.repository.Repository;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;
import com.couchbase.client.java.subdoc.LookupInBuilder;
import com.couchbase.client.java.subdoc.MutateInBuilder;
import com.couchbase.client.java.view.SpatialViewQuery;
import com.couchbase.client.java.view.SpatialViewResult;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import io.opentracing.Span;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TracingBucket implements Bucket {

  private final Bucket bucket;
  private final TracingHelper helper;

  public TracingBucket(Bucket bucket, TracingHelper helper) {
    this.bucket = bucket;
    this.helper = helper;
  }

  @Override
  public AsyncBucket async() {
    return bucket.async();
  }

  @Override
  public ClusterFacade core() {
    return bucket.core();
  }

  @Override
  public CouchbaseEnvironment environment() {
    return bucket.environment();
  }

  @Override
  public String name() {
    return bucket.name();
  }

  @Override
  public JsonDocument get(String id) {
    Span span = helper.buildSpan("get");
    span.setTag("id", nullable(id));
    try {
      return bucket.get(id);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument get(String id, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("get");
    span.setTag("id", nullable(id));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.get(id, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D get(D document) {
    Span span = helper.buildSpan("get");
    try {
      return bucket.get(document);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D get(D document, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("get");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.get(document, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D get(String id,
      Class<D> target) {
    Span span = helper.buildSpan("get");
    span.setTag("id", nullable(id));
    span.setTag("target", nullable(target));
    try {
      return bucket.get(id, target);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D get(String id,
      Class<D> target, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("get");
    span.setTag("id", nullable(id));
    span.setTag("target", nullable(target));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.get(id, target, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public boolean exists(String id) {
    Span span = helper.buildSpan("exists");
    span.setTag("id", nullable(id));
    try {
      return bucket.exists(id);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public boolean exists(String id, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("exists");
    span.setTag("id", nullable(id));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.exists(id, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> boolean exists(D document) {
    Span span = helper.buildSpan("exists");
    try {
      return bucket.exists(document);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> boolean exists(D document,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("exists");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.exists(document, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<JsonDocument> getFromReplica(
      String id, ReplicaMode type) {
    Span span = helper.buildSpan("getFromReplica");
    span.setTag("id", nullable(id));
    span.setTag("type", nullable(type));
    try {
      return bucket.getFromReplica(id, type);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Iterator<JsonDocument> getFromReplica(
      String id) {
    Span span = helper.buildSpan("getFromReplica");
    span.setTag("id", nullable(id));
    try {
      return bucket.getFromReplica(id);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<JsonDocument> getFromReplica(
      String id, ReplicaMode type, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("getFromReplica");
    span.setTag("id", nullable(id));
    span.setTag("type", nullable(type));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.getFromReplica(id, type, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Iterator<JsonDocument> getFromReplica(
      String id, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("getFromReplica");
    span.setTag("id", nullable(id));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.getFromReplica(id, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> List<D> getFromReplica(
      D document, ReplicaMode type) {
    Span span = helper.buildSpan("getFromReplica");
    span.setTag("type", nullable(type));
    try {
      return bucket.getFromReplica(document, type);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> Iterator<D> getFromReplica(
      D document) {
    Span span = helper.buildSpan("getFromReplica");
    try {
      return bucket.getFromReplica(document);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> List<D> getFromReplica(
      D document, ReplicaMode type, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("getFromReplica");
    span.setTag("type", nullable(type));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.getFromReplica(document, type, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> Iterator<D> getFromReplica(
      D document, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("getFromReplica");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.getFromReplica(document, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> List<D> getFromReplica(
      String id, ReplicaMode type, Class<D> target) {
    Span span = helper.buildSpan("getFromReplica");
    span.setTag("id", nullable(id));
    span.setTag("type", nullable(type));
    span.setTag("target", nullable(target));
    try {
      return bucket.getFromReplica(id, type, target);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> Iterator<D> getFromReplica(
      String id, Class<D> target) {
    Span span = helper.buildSpan("getFromReplica");
    span.setTag("id", nullable(id));
    span.setTag("target", nullable(target));
    try {
      return bucket.getFromReplica(id, target);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> List<D> getFromReplica(
      String id, ReplicaMode type, Class<D> target, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("getFromReplica");
    span.setTag("id", nullable(id));
    span.setTag("type", nullable(type));
    span.setTag("target", nullable(target));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.getFromReplica(id, type, target, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> Iterator<D> getFromReplica(
      String id, Class<D> target, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("getFromReplica");
    span.setTag("id", nullable(id));
    span.setTag("target", nullable(target));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.getFromReplica(id, target, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument getAndLock(String id, int lockTime) {
    Span span = helper.buildSpan("getAndLock");
    span.setTag("id", nullable(id));
    span.setTag("lockTime", lockTime);
    try {
      return bucket.getAndLock(id, lockTime);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument getAndLock(String id, int lockTime,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("getAndLock");
    span.setTag("id", nullable(id));
    span.setTag("lockTime", lockTime);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.getAndLock(id, lockTime, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D getAndLock(D document,
      int lockTime) {
    Span span = helper.buildSpan("getAndLock");
    span.setTag("lockTime", lockTime);
    try {
      return bucket.getAndLock(document, lockTime);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D getAndLock(D document,
      int lockTime, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("getAndLock");
    span.setTag("lockTime", lockTime);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.getAndLock(document, lockTime, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D getAndLock(String id,
      int lockTime, Class<D> target) {
    Span span = helper.buildSpan("getAndLock");
    span.setTag("id", nullable(id));
    span.setTag("lockTime", lockTime);
    span.setTag("target", nullable(target));
    try {
      return bucket.getAndLock(id, lockTime, target);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D getAndLock(String id,
      int lockTime, Class<D> target, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("getAndLock");
    span.setTag("id", nullable(id));
    span.setTag("lockTime", lockTime);
    span.setTag("target", nullable(target));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.getAndLock(id, lockTime, target, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument getAndTouch(String id, int expiry) {
    Span span = helper.buildSpan("getAndTouch");
    span.setTag("id", nullable(id));
    span.setTag("expiry", expiry);
    try {
      return bucket.getAndTouch(id, expiry);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument getAndTouch(String id, int expiry,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("getAndTouch");
    span.setTag("id", nullable(id));
    span.setTag("expiry", expiry);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.getAndTouch(id, expiry, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D getAndTouch(D document) {
    Span span = helper.buildSpan("getAndTouch");
    try {
      return bucket.getAndTouch(document);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D getAndTouch(D document,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("getAndTouch");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.getAndTouch(document, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D getAndTouch(String id,
      int expiry, Class<D> target) {
    Span span = helper.buildSpan("getAndTouch");
    span.setTag("id", nullable(id));
    span.setTag("expiry", expiry);
    span.setTag("target", nullable(target));
    try {
      return bucket.getAndTouch(id, expiry, target);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D getAndTouch(String id,
      int expiry, Class<D> target, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("getAndTouch");
    span.setTag("id", nullable(id));
    span.setTag("expiry", expiry);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("target", nullable(target));
    try {
      return bucket.getAndTouch(id, expiry, target, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D insert(D document) {
    Span span = helper.buildSpan("insert");
    try {
      return bucket.insert(document);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D insert(D document, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("insert");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.insert(document, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D insert(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("insert");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.insert(document, persistTo, replicateTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D insert(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("insert");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.insert(document, persistTo, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D insert(D document,
      PersistTo persistTo) {
    Span span = helper.buildSpan("insert");
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.insert(document, persistTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D insert(D document,
      PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("insert");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.insert(document, persistTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D insert(D document,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("insert");
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.insert(document, replicateTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D insert(D document,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("insert");
    span.setTag("replicateTo", nullable(replicateTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.insert(document, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D upsert(D document) {
    Span span = helper.buildSpan("upsert");
    try {
      return bucket.upsert(document);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D upsert(D document, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("upsert");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.upsert(document, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D upsert(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("upsert");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.upsert(document, persistTo, replicateTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D upsert(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("upsert");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.upsert(document, persistTo, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D upsert(D document,
      PersistTo persistTo) {
    Span span = helper.buildSpan("upsert");
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.upsert(document, persistTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D upsert(D document,
      PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("upsert");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.upsert(document, persistTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D upsert(D document,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("upsert");
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.upsert(document, replicateTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D upsert(D document,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("upsert");
    span.setTag("replicateTo", nullable(replicateTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.upsert(document, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D replace(D document) {
    Span span = helper.buildSpan("replace");
    try {
      return bucket.replace(document);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D replace(D document, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("replace");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.replace(document, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D replace(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("replace");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.replace(document, persistTo, replicateTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D replace(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("replace");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.replace(document, persistTo, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D replace(D document,
      PersistTo persistTo) {
    Span span = helper.buildSpan("replace");
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.replace(document, persistTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D replace(D document,
      PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("replace");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.replace(document, persistTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D replace(D document,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("replace");
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.replace(document, replicateTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D replace(D document,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("replace");
    span.setTag("replicateTo", nullable(replicateTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.replace(document, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(D document) {
    Span span = helper.buildSpan("remove");
    try {
      return bucket.remove(document);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(D document, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("remove");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.remove(document, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("remove");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.remove(document, persistTo, replicateTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("remove");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.remove(document, persistTo, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(D document,
      PersistTo persistTo) {
    Span span = helper.buildSpan("remove");
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.remove(document, persistTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(D document,
      PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("remove");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.remove(document, persistTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(D document,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("remove");
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.remove(document, replicateTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(D document,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("remove");
    span.setTag("replicateTo", nullable(replicateTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.remove(document, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument remove(String id) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    try {
      return bucket.remove(id);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument remove(String id, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.remove(id, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument remove(String id,
      PersistTo persistTo,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.remove(id, persistTo, replicateTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument remove(String id,
      PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.remove(id, persistTo, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument remove(String id,
      PersistTo persistTo) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.remove(id, persistTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument remove(String id,
      PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.remove(id, persistTo, timeout, timeUnit);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument remove(String id,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.remove(id, replicateTo);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument remove(String id,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.remove(id, replicateTo, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      Class<D> target) {
    return bucket.remove(id, target);
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      Class<D> target, long timeout, TimeUnit timeUnit) {
    return bucket.remove(id, target, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      PersistTo persistTo,
      ReplicateTo replicateTo, Class<D> target) {
    return bucket.remove(id, persistTo, replicateTo, target);
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      PersistTo persistTo,
      ReplicateTo replicateTo, Class<D> target, long timeout,
      TimeUnit timeUnit) {
    return bucket.remove(id, persistTo, replicateTo, target, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      PersistTo persistTo, Class<D> target) {
    return bucket.remove(id, persistTo, target);
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      PersistTo persistTo, Class<D> target, long timeout,
      TimeUnit timeUnit) {
    return bucket.remove(id, persistTo, target, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      ReplicateTo replicateTo, Class<D> target) {
    return bucket.remove(id, replicateTo, target);
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      ReplicateTo replicateTo, Class<D> target, long timeout,
      TimeUnit timeUnit) {
    return bucket.remove(id, replicateTo, target, timeout, timeUnit);
  }

  @Override
  public ViewResult query(
      ViewQuery query) {
    return bucket.query(query);
  }

  @Override
  public SpatialViewResult query(
      SpatialViewQuery query) {
    return bucket.query(query);
  }

  @Override
  public ViewResult query(
      ViewQuery query, long timeout,
      TimeUnit timeUnit) {
    return bucket.query(query, timeout, timeUnit);
  }

  @Override
  public SpatialViewResult query(
      SpatialViewQuery query, long timeout,
      TimeUnit timeUnit) {
    return bucket.query(query, timeout, timeUnit);
  }

  @Override
  public N1qlQueryResult query(
      Statement statement) {
    return bucket.query(statement);
  }

  @Override
  public N1qlQueryResult query(
      Statement statement, long timeout,
      TimeUnit timeUnit) {
    return bucket.query(statement, timeout, timeUnit);
  }

  @Override
  public N1qlQueryResult query(
      N1qlQuery query) {
    return bucket.query(query);
  }

  @Override
  public N1qlQueryResult query(
      N1qlQuery query, long timeout,
      TimeUnit timeUnit) {
    return bucket.query(query, timeout, timeUnit);
  }

  @Override
  @Committed
  public SearchQueryResult query(
      SearchQuery query) {
    return bucket.query(query);
  }

  @Override
  @Committed
  public SearchQueryResult query(
      SearchQuery query, long timeout,
      TimeUnit timeUnit) {
    return bucket.query(query, timeout, timeUnit);
  }

  @Override
  @Uncommitted
  public AnalyticsQueryResult query(
      AnalyticsQuery query) {
    return bucket.query(query);
  }

  @Override
  @Uncommitted
  public AnalyticsQueryResult query(
      AnalyticsQuery query, long timeout,
      TimeUnit timeUnit) {
    return bucket.query(query, timeout, timeUnit);
  }

  @Override
  public Boolean unlock(String id, long cas) {
    return bucket.unlock(id, cas);
  }

  @Override
  public Boolean unlock(String id, long cas, long timeout, TimeUnit timeUnit) {
    return bucket.unlock(id, cas, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> Boolean unlock(D document) {
    return bucket.unlock(document);
  }

  @Override
  public <D extends Document<?>> Boolean unlock(D document,
      long timeout, TimeUnit timeUnit) {
    return bucket.unlock(document, timeout, timeUnit);
  }

  @Override
  public Boolean touch(String id, int expiry) {
    return bucket.touch(id, expiry);
  }

  @Override
  public Boolean touch(String id, int expiry, long timeout, TimeUnit timeUnit) {
    return bucket.touch(id, expiry, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> Boolean touch(D document) {
    return bucket.touch(document);
  }

  @Override
  public <D extends Document<?>> Boolean touch(D document,
      long timeout, TimeUnit timeUnit) {
    return bucket.touch(document, timeout, timeUnit);
  }

  @Override
  public JsonLongDocument counter(String id, long delta) {
    return bucket.counter(id, delta);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      PersistTo persistTo) {
    return bucket.counter(id, delta, persistTo);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      ReplicateTo replicateTo) {
    return bucket.counter(id, delta, replicateTo);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      PersistTo persistTo,
      ReplicateTo replicateTo) {
    return bucket.counter(id, delta, persistTo, replicateTo);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long timeout, TimeUnit timeUnit) {
    return bucket.counter(id, delta, timeout, timeUnit);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.counter(id, delta, persistTo, timeout, timeUnit);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.counter(id, delta, replicateTo, timeout, timeUnit);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.counter(id, delta, persistTo, replicateTo, timeout, timeUnit);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial) {
    return bucket.counter(id, delta, initial);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, PersistTo persistTo) {
    return bucket.counter(id, delta, initial, persistTo);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, ReplicateTo replicateTo) {
    return bucket.counter(id, delta, initial, replicateTo);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, PersistTo persistTo,
      ReplicateTo replicateTo) {
    return bucket.counter(id, delta, initial, persistTo, replicateTo);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, long timeout, TimeUnit timeUnit) {
    return bucket.counter(id, delta, initial, timeout, timeUnit);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.counter(id, delta, initial, persistTo, timeout, timeUnit);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.counter(id, delta, initial, replicateTo, timeout, timeUnit);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.counter(id, delta, initial, persistTo, replicateTo, timeout, timeUnit);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry) {
    return bucket.counter(id, delta, initial, expiry);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, PersistTo persistTo) {
    return bucket.counter(id, delta, initial, expiry, persistTo);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, ReplicateTo replicateTo) {
    return bucket.counter(id, delta, initial, expiry, replicateTo);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, PersistTo persistTo,
      ReplicateTo replicateTo) {
    return bucket.counter(id, delta, initial, expiry, persistTo, replicateTo);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, long timeout, TimeUnit timeUnit) {
    return bucket.counter(id, delta, initial, expiry, timeout, timeUnit);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.counter(id, delta, initial, expiry, persistTo, timeout, timeUnit);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.counter(id, delta, initial, expiry, replicateTo, timeout, timeUnit);
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.counter(id, delta, initial, expiry, persistTo, replicateTo, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> D append(D document) {
    return bucket.append(document);
  }

  @Override
  public <D extends Document<?>> D append(D document,
      PersistTo persistTo) {
    return bucket.append(document, persistTo);
  }

  @Override
  public <D extends Document<?>> D append(D document,
      ReplicateTo replicateTo) {
    return bucket.append(document, replicateTo);
  }

  @Override
  public <D extends Document<?>> D append(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo) {
    return bucket.append(document, persistTo, replicateTo);
  }

  @Override
  public <D extends Document<?>> D append(D document, long timeout,
      TimeUnit timeUnit) {
    return bucket.append(document, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> D append(D document,
      PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.append(document, persistTo, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> D append(D document,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.append(document, replicateTo, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> D append(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.append(document, persistTo, replicateTo, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> D prepend(D document) {
    return bucket.prepend(document);
  }

  @Override
  public <D extends Document<?>> D prepend(D document,
      PersistTo persistTo) {
    return bucket.prepend(document, persistTo);
  }

  @Override
  public <D extends Document<?>> D prepend(D document,
      ReplicateTo replicateTo) {
    return bucket.prepend(document, replicateTo);
  }

  @Override
  public <D extends Document<?>> D prepend(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo) {
    return bucket.prepend(document, persistTo, replicateTo);
  }

  @Override
  public <D extends Document<?>> D prepend(D document, long timeout,
      TimeUnit timeUnit) {
    return bucket.prepend(document, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> D prepend(D document,
      PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.prepend(document, persistTo, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> D prepend(D document,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.prepend(document, replicateTo, timeout, timeUnit);
  }

  @Override
  public <D extends Document<?>> D prepend(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    return bucket.prepend(document, persistTo, replicateTo, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public LookupInBuilder lookupIn(String docId) {
    return bucket.lookupIn(docId);
  }

  @Override
  @Committed
  @Public
  public MutateInBuilder mutateIn(String docId) {
    return bucket.mutateIn(docId);
  }

  @Override
  @Committed
  @Public
  public <V> boolean mapAdd(String docId, String key, V value) {
    return bucket.mapAdd(docId, key, value);
  }

  @Override
  @Committed
  @Public
  public <V> boolean mapAdd(String docId, String key, V value, long timeout,
      TimeUnit timeUnit) {
    return bucket.mapAdd(docId, key, value, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <V> boolean mapAdd(String docId, String key, V value,
      MutationOptionBuilder mutationOptionBuilder) {
    return bucket.mapAdd(docId, key, value, mutationOptionBuilder);
  }

  @Override
  @Committed
  @Public
  public <V> boolean mapAdd(String docId, String key, V value,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    return bucket.mapAdd(docId, key, value, mutationOptionBuilder, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <V> V mapGet(String docId, String key, Class<V> valueType) {
    return bucket.mapGet(docId, key, valueType);
  }

  @Override
  @Committed
  @Public
  public <V> V mapGet(String docId, String key, Class<V> valueType, long timeout,
      TimeUnit timeUnit) {
    return bucket.mapGet(docId, key, valueType, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public boolean mapRemove(String docId, String key) {
    return bucket.mapRemove(docId, key);
  }

  @Override
  @Committed
  @Public
  public boolean mapRemove(String docId, String key, long timeout,
      TimeUnit timeUnit) {
    return bucket.mapRemove(docId, key, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public boolean mapRemove(String docId, String key,
      MutationOptionBuilder mutationOptionBuilder) {
    return bucket.mapRemove(docId, key, mutationOptionBuilder);
  }

  @Override
  @Committed
  @Public
  public boolean mapRemove(String docId, String key,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    return bucket.mapRemove(docId, key, mutationOptionBuilder, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public int mapSize(String docId) {
    return bucket.mapSize(docId);
  }

  @Override
  @Committed
  @Public
  public int mapSize(String docId, long timeout, TimeUnit timeUnit) {
    return bucket.mapSize(docId, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> E listGet(String docId, int index, Class<E> elementType) {
    return bucket.listGet(docId, index, elementType);
  }

  @Override
  @Committed
  @Public
  public <E> E listGet(String docId, int index, Class<E> elementType, long timeout,
      TimeUnit timeUnit) {
    return bucket.listGet(docId, index, elementType, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> boolean listAppend(String docId, E element) {
    return bucket.listAppend(docId, element);
  }

  @Override
  @Committed
  @Public
  public <E> boolean listAppend(String docId, E element, long timeout,
      TimeUnit timeUnit) {
    return bucket.listAppend(docId, element, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> boolean listAppend(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder) {
    return bucket.listAppend(docId, element, mutationOptionBuilder);
  }

  @Override
  @Committed
  @Public
  public <E> boolean listAppend(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    return bucket.listAppend(docId, element, mutationOptionBuilder, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public boolean listRemove(String docId, int index) {
    return bucket.listRemove(docId, index);
  }

  @Override
  @Committed
  @Public
  public boolean listRemove(String docId, int index, long timeout,
      TimeUnit timeUnit) {
    return bucket.listRemove(docId, index, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public boolean listRemove(String docId, int index,
      MutationOptionBuilder mutationOptionBuilder) {
    return bucket.listRemove(docId, index, mutationOptionBuilder);
  }

  @Override
  @Committed
  @Public
  public boolean listRemove(String docId, int index,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    return bucket.listRemove(docId, index, mutationOptionBuilder, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> boolean listPrepend(String docId, E element) {
    return bucket.listPrepend(docId, element);
  }

  @Override
  @Committed
  @Public
  public <E> boolean listPrepend(String docId, E element, long timeout,
      TimeUnit timeUnit) {
    return bucket.listPrepend(docId, element, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> boolean listPrepend(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder) {
    return bucket.listPrepend(docId, element, mutationOptionBuilder);
  }

  @Override
  @Committed
  @Public
  public <E> boolean listPrepend(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    return bucket.listPrepend(docId, element, mutationOptionBuilder, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> boolean listSet(String docId, int index, E element) {
    return bucket.listSet(docId, index, element);
  }

  @Override
  @Committed
  @Public
  public <E> boolean listSet(String docId, int index, E element, long timeout,
      TimeUnit timeUnit) {
    return bucket.listSet(docId, index, element, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> boolean listSet(String docId, int index, E element,
      MutationOptionBuilder mutationOptionBuilder) {
    return bucket.listSet(docId, index, element, mutationOptionBuilder);
  }

  @Override
  @Committed
  @Public
  public <E> boolean listSet(String docId, int index, E element,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    return bucket.listSet(docId, index, element, mutationOptionBuilder, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public int listSize(String docId) {
    return bucket.listSize(docId);
  }

  @Override
  @Committed
  @Public
  public int listSize(String docId, long timeout, TimeUnit timeUnit) {
    return bucket.listSize(docId, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> boolean setAdd(String docId, E element) {
    return bucket.setAdd(docId, element);
  }

  @Override
  @Committed
  @Public
  public <E> boolean setAdd(String docId, E element, long timeout,
      TimeUnit timeUnit) {
    return bucket.setAdd(docId, element, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> boolean setAdd(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder) {
    return bucket.setAdd(docId, element, mutationOptionBuilder);
  }

  @Override
  @Committed
  @Public
  public <E> boolean setAdd(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    return bucket.setAdd(docId, element, mutationOptionBuilder, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> boolean setContains(String docId, E element) {
    return bucket.setContains(docId, element);
  }

  @Override
  @Committed
  @Public
  public <E> boolean setContains(String docId, E element, long timeout,
      TimeUnit timeUnit) {
    return bucket.setContains(docId, element, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> E setRemove(String docId, E element) {
    return bucket.setRemove(docId, element);
  }

  @Override
  @Committed
  @Public
  public <E> E setRemove(String docId, E element, long timeout, TimeUnit timeUnit) {
    return bucket.setRemove(docId, element, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> E setRemove(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder) {
    return bucket.setRemove(docId, element, mutationOptionBuilder);
  }

  @Override
  @Committed
  @Public
  public <E> E setRemove(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    return bucket.setRemove(docId, element, mutationOptionBuilder, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public int setSize(String docId) {
    return bucket.setSize(docId);
  }

  @Override
  @Committed
  @Public
  public int setSize(String docId, long timeout, TimeUnit timeUnit) {
    return bucket.setSize(docId, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> boolean queuePush(String docId, E element) {
    return bucket.queuePush(docId, element);
  }

  @Override
  @Committed
  @Public
  public <E> boolean queuePush(String docId, E element, long timeout,
      TimeUnit timeUnit) {
    return bucket.queuePush(docId, element, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> boolean queuePush(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder) {
    return bucket.queuePush(docId, element, mutationOptionBuilder);
  }

  @Override
  @Committed
  @Public
  public <E> boolean queuePush(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    return bucket.queuePush(docId, element, mutationOptionBuilder, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> E queuePop(String docId, Class<E> elementType) {
    return bucket.queuePop(docId, elementType);
  }

  @Override
  @Committed
  @Public
  public <E> E queuePop(String docId, Class<E> elementType, long timeout,
      TimeUnit timeUnit) {
    return bucket.queuePop(docId, elementType, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public <E> E queuePop(String docId, Class<E> elementType,
      MutationOptionBuilder mutationOptionBuilder) {
    return bucket.queuePop(docId, elementType, mutationOptionBuilder);
  }

  @Override
  @Committed
  @Public
  public <E> E queuePop(String docId, Class<E> elementType,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    return bucket.queuePop(docId, elementType, mutationOptionBuilder, timeout, timeUnit);
  }

  @Override
  @Committed
  @Public
  public int queueSize(String docId) {
    return bucket.queueSize(docId);
  }

  @Override
  @Committed
  @Public
  public int queueSize(String docId, long timeout, TimeUnit timeUnit) {
    return bucket.queueSize(docId, timeout, timeUnit);
  }

  @Override
  public int invalidateQueryCache() {
    return bucket.invalidateQueryCache();
  }

  @Override
  public BucketManager bucketManager() {
    return bucket.bucketManager();
  }

  @Override
  @Public
  @Experimental
  public Repository repository() {
    return bucket.repository();
  }

  @Override
  public Boolean close() {
    return bucket.close();
  }

  @Override
  public Boolean close(long timeout, TimeUnit timeUnit) {
    return bucket.close(timeout, timeUnit);
  }

  @Override
  public boolean isClosed() {
    return bucket.isClosed();
  }

  @Override
  public PingReport ping(String reportId) {
    return bucket.ping(reportId);
  }

  @Override
  public PingReport ping(String reportId, long timeout,
      TimeUnit timeUnit) {
    return bucket.ping(reportId, timeout, timeUnit);
  }

  @Override
  public PingReport ping() {
    return bucket.ping();
  }

  @Override
  public PingReport ping(long timeout,
      TimeUnit timeUnit) {
    return bucket.ping(timeout, timeUnit);
  }

  @Override
  public PingReport ping(
      Collection<ServiceType> services) {
    return bucket.ping(services);
  }

  @Override
  public PingReport ping(
      Collection<ServiceType> services, long timeout,
      TimeUnit timeUnit) {
    return bucket.ping(services, timeout, timeUnit);
  }

  @Override
  public PingReport ping(String reportId,
      Collection<ServiceType> services) {
    return bucket.ping(reportId, services);
  }

  @Override
  public PingReport ping(String reportId,
      Collection<ServiceType> services, long timeout,
      TimeUnit timeUnit) {
    return bucket.ping(reportId, services, timeout, timeUnit);
  }


}
