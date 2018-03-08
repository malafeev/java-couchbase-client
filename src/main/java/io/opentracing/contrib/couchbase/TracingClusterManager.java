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
import static io.opentracing.contrib.couchbase.TracingHelper.onError;

import com.couchbase.client.core.annotations.InterfaceStability.Experimental;
import com.couchbase.client.java.cluster.AsyncClusterManager;
import com.couchbase.client.java.cluster.AuthDomain;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.User;
import com.couchbase.client.java.cluster.UserSettings;
import com.couchbase.client.java.cluster.api.ClusterApiClient;
import io.opentracing.Span;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TracingClusterManager implements ClusterManager {

  private final ClusterManager clusterManager;
  private final TracingHelper helper;

  public TracingClusterManager(ClusterManager clusterManager, TracingHelper helper) {
    this.clusterManager = clusterManager;
    this.helper = helper;
  }

  @Override
  public AsyncClusterManager async() {
    return clusterManager.async();
  }

  @Override
  public ClusterInfo info() {
    Span span = helper.buildSpan("info");
    try {
      return clusterManager.info();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public ClusterInfo info(long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("info");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return clusterManager.info(timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<BucketSettings> getBuckets() {
    Span span = helper.buildSpan("getBuckets");
    try {
      return clusterManager.getBuckets();
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public List<BucketSettings> getBuckets(long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("getBuckets");
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return clusterManager.getBuckets(timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public BucketSettings getBucket(String name) {
    Span span = helper.buildSpan("getBucket");
    span.setTag("name", nullable(name));
    try {
      return clusterManager.getBucket(name);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public BucketSettings getBucket(String name, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("getBucket");
    span.setTag("name", nullable(name));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return clusterManager.getBucket(name, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean hasBucket(String name) {
    Span span = helper.buildSpan("hasBucket");
    span.setTag("name", nullable(name));
    try {
      return clusterManager.hasBucket(name);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean hasBucket(String name, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("hasBucket");
    span.setTag("name", nullable(name));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return clusterManager.hasBucket(name, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public BucketSettings insertBucket(
      BucketSettings settings) {
    Span span = helper.buildSpan("insertBucket");
    span.setTag("settings", nullable(settings));
    try {
      return clusterManager.insertBucket(settings);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public BucketSettings insertBucket(
      BucketSettings settings, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("insertBucket");
    span.setTag("settings", nullable(settings));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return clusterManager.insertBucket(settings, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public BucketSettings updateBucket(
      BucketSettings settings) {
    Span span = helper.buildSpan("updateBucket");
    span.setTag("settings", nullable(settings));
    try {
      return clusterManager.updateBucket(settings);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public BucketSettings updateBucket(
      BucketSettings settings, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("updateBucket");
    span.setTag("settings", nullable(settings));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return clusterManager.updateBucket(settings, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean removeBucket(String name) {
    Span span = helper.buildSpan("removeBucket");
    span.setTag("name", nullable(name));
    try {
      return clusterManager.removeBucket(name);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  public Boolean removeBucket(String name, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("removeBucket");
    span.setTag("name", nullable(name));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return clusterManager.removeBucket(name, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Experimental
  public Boolean upsertUser(AuthDomain domain,
      String username, UserSettings settings) {
    Span span = helper.buildSpan("upsertUser");
    span.setTag("domain", nullable(domain));
    span.setTag("username", nullable(username));
    try {
      return clusterManager.upsertUser(domain, username, settings);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Experimental
  public Boolean upsertUser(AuthDomain domain,
      String username, UserSettings settings, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("upsertUser");
    span.setTag("domain", nullable(domain));
    span.setTag("username", nullable(username));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return clusterManager.upsertUser(domain, username, settings, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Experimental
  public Boolean removeUser(AuthDomain domain,
      String username) {
    Span span = helper.buildSpan("removeUser");
    span.setTag("domain", nullable(domain));
    span.setTag("username", nullable(username));
    try {
      return clusterManager.removeUser(domain, username);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Experimental
  public Boolean removeUser(AuthDomain domain,
      String username, long timeout, TimeUnit timeUnit) {
    Span span = helper.buildSpan("removeUser");
    span.setTag("domain", nullable(domain));
    span.setTag("username", nullable(username));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return clusterManager.removeUser(domain, username, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Experimental
  public List<User> getUsers(
      AuthDomain domain) {
    Span span = helper.buildSpan("getUsers");
    span.setTag("domain", nullable(domain));
    try {
      return clusterManager.getUsers(domain);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Experimental
  public List<User> getUsers(
      AuthDomain domain, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("getUsers");
    span.setTag("domain", nullable(domain));
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return clusterManager.getUsers(domain, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Experimental
  public User getUser(
      AuthDomain domain, String userid) {
    Span span = helper.buildSpan("getUser");
    span.setTag("domain", nullable(domain));
    span.setTag("userid", userid);
    try {
      return clusterManager.getUser(domain, userid);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Experimental
  public User getUser(
      AuthDomain domain, String userid, long timeout,
      TimeUnit timeUnit) {
    Span span = helper.buildSpan("getUser");
    span.setTag("domain", nullable(domain));
    span.setTag("userid", userid);
    span.setTag("timeout", timeout);
    span.setTag("timeUnit", nullable(timeUnit));
    try {
      return clusterManager.getUser(domain, userid, timeout, timeUnit);
    } catch (Exception e) {
      onError(e, span);
      throw e;
    } finally {
      span.finish();
    }
  }

  @Override
  @Experimental
  public ClusterApiClient apiClient() {
    return clusterManager.apiClient();
  }
}
