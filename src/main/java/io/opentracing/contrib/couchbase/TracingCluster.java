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
import com.couchbase.client.core.annotations.InterfaceStability.Experimental;
import com.couchbase.client.core.annotations.InterfaceStability.Uncommitted;
import com.couchbase.client.core.message.internal.DiagnosticsReport;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.auth.Authenticator;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.document.Document;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.transcoder.Transcoder;
import io.opentracing.Span;
import io.opentracing.Tracer;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TracingCluster implements Cluster {

  private final Cluster cluster;
  private final TracingHelper helper;
  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;

  public TracingCluster(Cluster cluster, Tracer tracer, boolean traceWithActiveSpanOnly) {
    this.cluster = cluster;
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
    this.helper = new TracingHelper(tracer, traceWithActiveSpanOnly);
  }

  @Override
  public Bucket openBucket() {
    Span span = helper.buildSpan("openBucket");
    try {
      return new TracingBucket(cluster.openBucket(), helper);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Bucket openBucket(long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("openBucket");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return new TracingBucket(cluster.openBucket(timeout, timeUnit), helper);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Bucket openBucket(String name) {
    Span span = helper.buildSpan("openBucket");
    span.setTag("name", name);
    try {
      return new TracingBucket(cluster.openBucket(name), helper);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Bucket openBucket(String name, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("openBucket");
    span.setTag("name", name);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return new TracingBucket(cluster.openBucket(name, timeout, timeUnit), helper);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Bucket openBucket(String name,
      List<Transcoder<? extends Document, ?>> transcoders) {
    Span span = helper.buildSpan("openBucket");
    span.setTag("name", name);
    span.setTag("transcoders", TracingHelper.toStringClass(transcoders));
    try {
      return new TracingBucket(cluster.openBucket(name, transcoders), helper);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Bucket openBucket(String name,
      List<Transcoder<? extends Document, ?>> transcoders,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("openBucket");
    span.setTag("name", name);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("transcoders", TracingHelper.toStringClass(transcoders));
    try {
      return new TracingBucket(cluster.openBucket(name, transcoders, timeout, timeUnit), helper);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Bucket openBucket(String name, String password) {
    Span span = helper.buildSpan("openBucket");
    span.setTag("name", name);
    try {
      return new TracingBucket(cluster.openBucket(name, password), helper);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Bucket openBucket(String name, String password, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("openBucket");
    span.setTag("name", name);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return new TracingBucket(cluster.openBucket(name, password, timeout, timeUnit), helper);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Bucket openBucket(String name, String password,
      List<Transcoder<? extends Document, ?>> transcoders) {
    Span span = helper.buildSpan("openBucket");
    span.setTag("name", name);
    span.setTag("transcoders", TracingHelper.toStringClass(transcoders));
    try {
      return new TracingBucket(cluster.openBucket(name, password, transcoders), helper);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Bucket openBucket(String name, String password,
      List<Transcoder<? extends Document, ?>> transcoders,
      long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("openBucket");
    span.setTag("name", name);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    span.setTag("transcoders", TracingHelper.toStringClass(transcoders));
    try {
      return new TracingBucket(cluster.openBucket(name, password, transcoders, timeout, timeUnit),
          helper);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Uncommitted
  public N1qlQueryResult query(
      N1qlQuery query) {
    Span span = helper.buildSpan("query");
    span.setTag("query", nullableClass(query));
    try {
      return cluster.query(query);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Uncommitted
  public N1qlQueryResult query(
      N1qlQuery query, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("query");
    span.setTag("query", nullableClass(query));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return cluster.query(query, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ClusterManager clusterManager(String username,
      String password) {
    return new TracingClusterManager(cluster.clusterManager(username, password), helper);
  }

  @Override
  public ClusterManager clusterManager() {
    return new TracingClusterManager(cluster.clusterManager(), helper);
  }

  @Override
  public Boolean disconnect() {
    Span span = helper.buildSpan("disconnect");
    try {
      return cluster.disconnect();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean disconnect(long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("disconnect");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return cluster.disconnect(timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ClusterFacade core() {
    // TODO: tracing ClusterFacade
    return cluster.core();
  }

  @Override
  public Cluster authenticate(Authenticator auth) {
    return new TracingCluster(cluster.authenticate(auth), tracer, traceWithActiveSpanOnly);
  }

  @Override
  public Cluster authenticate(String username, String password) {
    return new TracingCluster(cluster.authenticate(username, password), tracer,
        traceWithActiveSpanOnly);
  }

  @Override
  @Experimental
  @Public
  public DiagnosticsReport diagnostics() {
    Span span = helper.buildSpan("diagnostics");
    try {
      return cluster.diagnostics();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Experimental
  @Public
  public DiagnosticsReport diagnostics(
      String reportId) {
    Span span = helper.buildSpan("diagnostics");
    span.setTag("reportId", nullable(reportId));
    try {
      return cluster.diagnostics(reportId);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }
}
