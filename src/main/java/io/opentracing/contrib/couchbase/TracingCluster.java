package io.opentracing.contrib.couchbase;

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
      TracingHelper.onError(e, span);
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
    span.setTag("timeUnit", TracingHelper.nullable(timeUnit));
    try {
      return new TracingBucket(cluster.openBucket(timeout, timeUnit), helper);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
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
      TracingHelper.onError(e, span);
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
    span.setTag("timeUnit", TracingHelper.nullable(timeUnit));
    try {
      return new TracingBucket(cluster.openBucket(name, timeout, timeUnit), helper);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
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
    span.setTag("transcoders", TracingHelper.toString(transcoders));
    try {
      return new TracingBucket(cluster.openBucket(name, transcoders), helper);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
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
    span.setTag("timeUnit", TracingHelper.nullable(timeUnit));
    span.setTag("transcoders", TracingHelper.toString(transcoders));
    try {
      return new TracingBucket(cluster.openBucket(name, transcoders, timeout, timeUnit), helper);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
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
      TracingHelper.onError(e, span);
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
    span.setTag("timeUnit", TracingHelper.nullable(timeUnit));
    try {
      return new TracingBucket(cluster.openBucket(name, password, timeout, timeUnit), helper);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
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
    span.setTag("transcoders", TracingHelper.toString(transcoders));
    try {
      return new TracingBucket(cluster.openBucket(name, password, transcoders), helper);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
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
    span.setTag("timeUnit", TracingHelper.nullable(timeUnit));
    span.setTag("transcoders", TracingHelper.toString(transcoders));
    try {
      return new TracingBucket(cluster.openBucket(name, password, transcoders, timeout, timeUnit), helper);
    } catch (Exception e) {
      TracingHelper.onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Uncommitted
  public N1qlQueryResult query(
      N1qlQuery query) {
    return cluster.query(query);
  }

  @Override
  @Uncommitted
  public N1qlQueryResult query(
      N1qlQuery query, long timeout,
      TimeUnit timeUnit) {
    return cluster.query(query, timeout, timeUnit);
  }

  @Override
  public ClusterManager clusterManager(String username,
      String password) {
    return new TracingClusterManager(cluster.clusterManager(username, password));
  }

  @Override
  public ClusterManager clusterManager() {
    return new TracingClusterManager(cluster.clusterManager());
  }

  @Override
  public Boolean disconnect() {
    return cluster.disconnect();
  }

  @Override
  public Boolean disconnect(long timeout, TimeUnit timeUnit) {
    return cluster.disconnect(timeout, timeUnit);
  }

  @Override
  public ClusterFacade core() {
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
    return cluster.diagnostics();
  }

  @Override
  @Experimental
  @Public
  public DiagnosticsReport diagnostics(
      String reportId) {
    return cluster.diagnostics(reportId);
  }
}
