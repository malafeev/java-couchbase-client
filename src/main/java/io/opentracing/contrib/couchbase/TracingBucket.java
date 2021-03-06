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

import static io.opentracing.contrib.couchbase.TracingHelper.nullable;
import static io.opentracing.contrib.couchbase.TracingHelper.nullableClass;
import static io.opentracing.contrib.couchbase.TracingHelper.onError;

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
    // TODO: tracing AsyncBucket?
    return bucket.async();
  }

  @Override
  public ClusterFacade core() {
    // TODO: tracing ClusterFacade?
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
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
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonDocument remove(String id,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("replicateTo", nullable(replicateTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.remove(id, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      Class<D> target) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("target", nullable(target));
    try {
      return bucket.remove(id, target);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      Class<D> target, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("target", nullable(target));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.remove(id, target, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      PersistTo persistTo,
      ReplicateTo replicateTo, Class<D> target) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("target", nullable(target));
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.remove(id, persistTo, replicateTo, target);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      PersistTo persistTo,
      ReplicateTo replicateTo, Class<D> target, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("target", nullable(target));
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.remove(id, persistTo, replicateTo, target, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      PersistTo persistTo, Class<D> target) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("target", nullable(target));
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.remove(id, persistTo, target);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      PersistTo persistTo, Class<D> target, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("target", nullable(target));
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.remove(id, persistTo, target, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      ReplicateTo replicateTo, Class<D> target) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("target", nullable(target));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.remove(id, replicateTo, target);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D remove(String id,
      ReplicateTo replicateTo, Class<D> target, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("remove");
    span.setTag("id", nullable(id));
    span.setTag("target", nullable(target));
    span.setTag("replicateTo", nullable(replicateTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.remove(id, replicateTo, target, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ViewResult query(
      ViewQuery query) {
    Span span = helper.buildSpan("query");
    span.setTag("query", nullable(query));
    try {
      return bucket.query(query);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public SpatialViewResult query(
      SpatialViewQuery query) {
    Span span = helper.buildSpan("query");
    span.setTag("query", nullable(query));
    try {
      return bucket.query(query);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ViewResult query(
      ViewQuery query, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("query");
    span.setTag("query", nullable(query));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.query(query, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public SpatialViewResult query(
      SpatialViewQuery query, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("query");
    span.setTag("query", nullable(query));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.query(query, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public N1qlQueryResult query(
      Statement statement) {
    Span span = helper.buildSpan("query");
    span.setTag("statement", nullableClass(statement));
    try {
      return bucket.query(statement);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public N1qlQueryResult query(
      Statement statement, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("query");
    span.setTag("statement", nullableClass(statement));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.query(statement, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public N1qlQueryResult query(
      N1qlQuery query) {
    Span span = helper.buildSpan("query");
    span.setTag("query", nullable(query));
    try {
      return bucket.query(query);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public N1qlQueryResult query(
      N1qlQuery query, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("query");
    span.setTag("query", nullable(query));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.query(query, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  public SearchQueryResult query(
      SearchQuery query) {
    Span span = helper.buildSpan("query");
    span.setTag("query", nullable(query));
    try {
      return bucket.query(query);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  public SearchQueryResult query(
      SearchQuery query, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("query");
    span.setTag("query", nullable(query));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.query(query, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Uncommitted
  public AnalyticsQueryResult query(
      AnalyticsQuery query) {
    Span span = helper.buildSpan("query");
    span.setTag("query", nullable(query));
    try {
      return bucket.query(query);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Uncommitted
  public AnalyticsQueryResult query(
      AnalyticsQuery query, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("query");
    span.setTag("query", nullable(query));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.query(query, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean unlock(String id, long cas) {
    Span span = helper.buildSpan("unlock");
    span.setTag("id", nullable(id));
    span.setTag("cas", cas);
    try {
      return bucket.unlock(id, cas);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean unlock(String id, long cas, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("unlock");
    span.setTag("id", nullable(id));
    span.setTag("cas", cas);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.unlock(id, cas, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> Boolean unlock(D document) {
    Span span = helper.buildSpan("unlock");
    try {
      return bucket.unlock(document);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> Boolean unlock(D document,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("unlock");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.unlock(document, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean touch(String id, int expiry) {
    Span span = helper.buildSpan("touch");
    span.setTag("id", nullable(id));
    span.setTag("expiry", expiry);
    try {
      return bucket.touch(id, expiry);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean touch(String id, int expiry, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("touch");
    span.setTag("id", nullable(id));
    span.setTag("expiry", expiry);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.touch(id, expiry, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> Boolean touch(D document) {
    Span span = helper.buildSpan("touch");
    try {
      return bucket.touch(document);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> Boolean touch(D document,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("touch");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.touch(document, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    try {
      return bucket.counter(id, delta);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      PersistTo persistTo) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.counter(id, delta, persistTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.counter(id, delta, replicateTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      PersistTo persistTo,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.counter(id, delta, persistTo, replicateTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.counter(id, delta, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.counter(id, delta, persistTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.counter(id, delta, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.counter(id, delta, persistTo, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    try {
      return bucket.counter(id, delta, initial);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, PersistTo persistTo) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.counter(id, delta, initial, persistTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, ReplicateTo replicateTo) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.counter(id, delta, initial, replicateTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, PersistTo persistTo,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.counter(id, delta, initial, persistTo, replicateTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.counter(id, delta, initial, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.counter(id, delta, initial, persistTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.counter(id, delta, initial, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.counter(id, delta, initial, persistTo, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("expiry", expiry);
    try {
      return bucket.counter(id, delta, initial, expiry);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, PersistTo persistTo) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("expiry", expiry);
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.counter(id, delta, initial, expiry, persistTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, ReplicateTo replicateTo) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("expiry", expiry);
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.counter(id, delta, initial, expiry, replicateTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, PersistTo persistTo,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("expiry", expiry);
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.counter(id, delta, initial, expiry, persistTo, replicateTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("expiry", expiry);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.counter(id, delta, initial, expiry, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("expiry", expiry);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.counter(id, delta, initial, expiry, persistTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("expiry", expiry);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.counter(id, delta, initial, expiry, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public JsonLongDocument counter(String id, long delta,
      long initial, int expiry, PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("counter");
    span.setTag("id", nullable(id));
    span.setTag("delta", delta);
    span.setTag("initial", initial);
    span.setTag("expiry", expiry);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.counter(id, delta, initial, expiry, persistTo, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D append(D document) {
    Span span = helper.buildSpan("append");
    try {
      return bucket.append(document);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D append(D document,
      PersistTo persistTo) {
    Span span = helper.buildSpan("append");
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.append(document, persistTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D append(D document,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("append");
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.append(document, replicateTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D append(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("append");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.append(document, persistTo, replicateTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D append(D document, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("append");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.append(document, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D append(D document,
      PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("append");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.append(document, persistTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D append(D document,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("append");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.append(document, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D append(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("append");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.append(document, persistTo, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D prepend(D document) {
    Span span = helper.buildSpan("prepend");
    try {
      return bucket.prepend(document);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D prepend(D document,
      PersistTo persistTo) {
    Span span = helper.buildSpan("prepend");
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.prepend(document, persistTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D prepend(D document,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("prepend");
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.prepend(document, replicateTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D prepend(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo) {
    Span span = helper.buildSpan("prepend");
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.prepend(document, persistTo, replicateTo);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D prepend(D document, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("prepend");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.prepend(document, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D prepend(D document,
      PersistTo persistTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("prepend");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("persistTo", nullable(persistTo));
    try {
      return bucket.prepend(document, persistTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D prepend(D document,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("prepend");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.prepend(document, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public <D extends Document<?>> D prepend(D document,
      PersistTo persistTo,
      ReplicateTo replicateTo, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("prepend");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("persistTo", nullable(persistTo));
    span.setTag("replicateTo", nullable(replicateTo));
    try {
      return bucket.prepend(document, persistTo, replicateTo, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
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
    Span span = helper.buildSpan("mapAdd");
    span.setTag("docId", nullable(docId));
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    try {
      return bucket.mapAdd(docId, key, value);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <V> boolean mapAdd(String docId, String key, V value, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("mapAdd");
    span.setTag("docId", nullable(docId));
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.mapAdd(docId, key, value, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <V> boolean mapAdd(String docId, String key, V value,
      MutationOptionBuilder mutationOptionBuilder) {
    Span span = helper.buildSpan("mapAdd");
    span.setTag("docId", nullable(docId));
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    try {
      return bucket.mapAdd(docId, key, value, mutationOptionBuilder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <V> boolean mapAdd(String docId, String key, V value,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("mapAdd");
    span.setTag("docId", nullable(docId));
    span.setTag("key", nullable(key));
    span.setTag("value", nullable(value));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.mapAdd(docId, key, value, mutationOptionBuilder, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <V> V mapGet(String docId, String key, Class<V> valueType) {
    Span span = helper.buildSpan("mapGet");
    span.setTag("docId", nullable(docId));
    span.setTag("key", nullable(key));
    span.setTag("valueType", nullable(valueType));
    try {
      return bucket.mapGet(docId, key, valueType);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <V> V mapGet(String docId, String key, Class<V> valueType, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("mapGet");
    span.setTag("docId", nullable(docId));
    span.setTag("key", nullable(key));
    span.setTag("valueType", nullable(valueType));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.mapGet(docId, key, valueType, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public boolean mapRemove(String docId, String key) {
    Span span = helper.buildSpan("mapRemove");
    span.setTag("docId", nullable(docId));
    span.setTag("key", nullable(key));
    try {
      return bucket.mapRemove(docId, key);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public boolean mapRemove(String docId, String key, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("mapRemove");
    span.setTag("docId", nullable(docId));
    span.setTag("key", nullable(key));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.mapRemove(docId, key, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public boolean mapRemove(String docId, String key,
      MutationOptionBuilder mutationOptionBuilder) {
    Span span = helper.buildSpan("mapRemove");
    span.setTag("docId", nullable(docId));
    span.setTag("key", nullable(key));
    try {
      return bucket.mapRemove(docId, key, mutationOptionBuilder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public boolean mapRemove(String docId, String key,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("mapRemove");
    span.setTag("docId", nullable(docId));
    span.setTag("key", nullable(key));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.mapRemove(docId, key, mutationOptionBuilder, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public int mapSize(String docId) {
    Span span = helper.buildSpan("mapSize");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.mapSize(docId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public int mapSize(String docId, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("mapSize");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.mapSize(docId, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> E listGet(String docId, int index, Class<E> elementType) {
    Span span = helper.buildSpan("listGet");
    span.setTag("docId", nullable(docId));
    span.setTag("index", index);
    span.setTag("elementType", nullable(elementType));
    try {
      return bucket.listGet(docId, index, elementType);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> E listGet(String docId, int index, Class<E> elementType, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("listGet");
    span.setTag("docId", nullable(docId));
    span.setTag("index", index);
    span.setTag("elementType", nullable(elementType));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.listGet(docId, index, elementType, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean listAppend(String docId, E element) {
    Span span = helper.buildSpan("listAppend");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.listAppend(docId, element);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean listAppend(String docId, E element, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("listAppend");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.listAppend(docId, element, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean listAppend(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder) {
    Span span = helper.buildSpan("listAppend");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.listAppend(docId, element, mutationOptionBuilder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean listAppend(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("listAppend");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.listAppend(docId, element, mutationOptionBuilder, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public boolean listRemove(String docId, int index) {
    Span span = helper.buildSpan("listRemove");
    span.setTag("docId", nullable(docId));
    span.setTag("index", index);
    try {
      return bucket.listRemove(docId, index);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public boolean listRemove(String docId, int index, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("listRemove");
    span.setTag("docId", nullable(docId));
    span.setTag("index", index);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.listRemove(docId, index, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public boolean listRemove(String docId, int index,
      MutationOptionBuilder mutationOptionBuilder) {
    Span span = helper.buildSpan("listRemove");
    span.setTag("docId", nullable(docId));
    span.setTag("index", index);
    try {
      return bucket.listRemove(docId, index, mutationOptionBuilder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public boolean listRemove(String docId, int index,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("listRemove");
    span.setTag("docId", nullable(docId));
    span.setTag("index", index);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.listRemove(docId, index, mutationOptionBuilder, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean listPrepend(String docId, E element) {
    Span span = helper.buildSpan("listPrepend");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.listPrepend(docId, element);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean listPrepend(String docId, E element, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("listPrepend");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.listPrepend(docId, element, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean listPrepend(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder) {
    Span span = helper.buildSpan("listPrepend");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.listPrepend(docId, element, mutationOptionBuilder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean listPrepend(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("listPrepend");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.listPrepend(docId, element, mutationOptionBuilder, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean listSet(String docId, int index, E element) {
    Span span = helper.buildSpan("listSet");
    span.setTag("docId", nullable(docId));
    span.setTag("index", index);
    try {
      return bucket.listSet(docId, index, element);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean listSet(String docId, int index, E element, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("listSet");
    span.setTag("docId", nullable(docId));
    span.setTag("index", index);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.listSet(docId, index, element, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean listSet(String docId, int index, E element,
      MutationOptionBuilder mutationOptionBuilder) {
    Span span = helper.buildSpan("listSet");
    span.setTag("docId", nullable(docId));
    span.setTag("index", index);
    try {
      return bucket.listSet(docId, index, element, mutationOptionBuilder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean listSet(String docId, int index, E element,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("listSet");
    span.setTag("docId", nullable(docId));
    span.setTag("index", index);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.listSet(docId, index, element, mutationOptionBuilder, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public int listSize(String docId) {
    Span span = helper.buildSpan("listSize");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.listSize(docId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public int listSize(String docId, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("listSize");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.listSize(docId, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean setAdd(String docId, E element) {
    Span span = helper.buildSpan("setAdd");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.setAdd(docId, element);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean setAdd(String docId, E element, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("setAdd");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.setAdd(docId, element, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean setAdd(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder) {
    Span span = helper.buildSpan("setAdd");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.setAdd(docId, element, mutationOptionBuilder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean setAdd(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("setAdd");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.setAdd(docId, element, mutationOptionBuilder, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean setContains(String docId, E element) {
    Span span = helper.buildSpan("setContains");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.setContains(docId, element);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean setContains(String docId, E element, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("setContains");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.setContains(docId, element, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> E setRemove(String docId, E element) {
    Span span = helper.buildSpan("setRemove");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.setRemove(docId, element);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> E setRemove(String docId, E element, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("setRemove");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.setRemove(docId, element, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> E setRemove(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder) {
    Span span = helper.buildSpan("setRemove");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.setRemove(docId, element, mutationOptionBuilder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> E setRemove(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("setRemove");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.setRemove(docId, element, mutationOptionBuilder, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public int setSize(String docId) {
    Span span = helper.buildSpan("setSize");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.setSize(docId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public int setSize(String docId, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("setSize");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.setSize(docId, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean queuePush(String docId, E element) {
    Span span = helper.buildSpan("queuePush");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.queuePush(docId, element);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean queuePush(String docId, E element, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("queuePush");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.queuePush(docId, element, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean queuePush(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder) {
    Span span = helper.buildSpan("queuePush");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.queuePush(docId, element, mutationOptionBuilder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> boolean queuePush(String docId, E element,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("queuePush");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.queuePush(docId, element, mutationOptionBuilder, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> E queuePop(String docId, Class<E> elementType) {
    Span span = helper.buildSpan("queuePop");
    span.setTag("docId", nullable(docId));
    span.setTag("elementType", nullable(elementType));
    try {
      return bucket.queuePop(docId, elementType);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> E queuePop(String docId, Class<E> elementType, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("queuePop");
    span.setTag("docId", nullable(docId));
    span.setTag("elementType", nullable(elementType));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.queuePop(docId, elementType, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> E queuePop(String docId, Class<E> elementType,
      MutationOptionBuilder mutationOptionBuilder) {
    Span span = helper.buildSpan("queuePop");
    span.setTag("docId", nullable(docId));
    span.setTag("elementType", nullable(elementType));
    try {
      return bucket.queuePop(docId, elementType, mutationOptionBuilder);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public <E> E queuePop(String docId, Class<E> elementType,
      MutationOptionBuilder mutationOptionBuilder,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("queuePop");
    span.setTag("docId", nullable(docId));
    span.setTag("elementType", nullable(elementType));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.queuePop(docId, elementType, mutationOptionBuilder, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public int queueSize(String docId) {
    Span span = helper.buildSpan("queueSize");
    span.setTag("docId", nullable(docId));
    try {
      return bucket.queueSize(docId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Committed
  @Public
  public int queueSize(String docId, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("queueSize");
    span.setTag("docId", nullable(docId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.queueSize(docId, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public int invalidateQueryCache() {
    Span span = helper.buildSpan("invalidateQueryCache");
    try {
      return bucket.invalidateQueryCache();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public BucketManager bucketManager() {
    // TODO: tracing BucketManager ?
    return bucket.bucketManager();
  }

  @Override
  @Public
  @Experimental
  public Repository repository() {
    // TODO: tracing Repository?
    return bucket.repository();
  }

  @Override
  public Boolean close() {
    Span span = helper.buildSpan("close");
    try {
      return bucket.close();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean close(long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("close");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.close(timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public boolean isClosed() {
    return bucket.isClosed();
  }

  @Override
  public PingReport ping(String reportId) {
    Span span = helper.buildSpan("ping");
    span.setTag("reportId", nullable(reportId));
    try {
      return bucket.ping(reportId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public PingReport ping(String reportId, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("ping");
    span.setTag("reportId", nullable(reportId));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.ping(reportId, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public PingReport ping() {
    Span span = helper.buildSpan("ping");
    try {
      return bucket.ping();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public PingReport ping(long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("ping");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.ping(timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public PingReport ping(
      Collection<ServiceType> services) {
    Span span = helper.buildSpan("ping");
    span.setTag("services", TracingHelper.toString(services));
    try {
      return bucket.ping(services);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public PingReport ping(
      Collection<ServiceType> services, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("ping");
    span.setTag("services", TracingHelper.toString(services));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.ping(services, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public PingReport ping(String reportId,
      Collection<ServiceType> services) {
    Span span = helper.buildSpan("ping");
    span.setTag("reportId", nullable(reportId));
    span.setTag("services", TracingHelper.toString(services));
    try {
      return bucket.ping(reportId, services);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public PingReport ping(String reportId,
      Collection<ServiceType> services, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("ping");
    span.setTag("reportId", nullable(reportId));
    span.setTag("services", TracingHelper.toString(services));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return bucket.ping(reportId, services, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }
}
