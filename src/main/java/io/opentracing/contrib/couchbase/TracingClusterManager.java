package io.opentracing.contrib.couchbase;

import com.couchbase.client.core.annotations.InterfaceStability.Experimental;
import com.couchbase.client.java.cluster.AsyncClusterManager;
import com.couchbase.client.java.cluster.AuthDomain;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterInfo;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.User;
import com.couchbase.client.java.cluster.UserSettings;
import com.couchbase.client.java.cluster.api.ClusterApiClient;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class TracingClusterManager implements ClusterManager {
  private final ClusterManager clusterManager;

  public TracingClusterManager(ClusterManager clusterManager) {
    this.clusterManager = clusterManager;
  }

  @Override
  public AsyncClusterManager async() {
    return clusterManager.async();
  }

  @Override
  public ClusterInfo info() {
    return clusterManager.info();
  }

  @Override
  public ClusterInfo info(long timeout,
      TimeUnit timeUnit) {
    return clusterManager.info(timeout, timeUnit);
  }

  @Override
  public List<BucketSettings> getBuckets() {
    return clusterManager.getBuckets();
  }

  @Override
  public List<BucketSettings> getBuckets(long timeout,
      TimeUnit timeUnit) {
    return clusterManager.getBuckets(timeout, timeUnit);
  }

  @Override
  public BucketSettings getBucket(String name) {
    return clusterManager.getBucket(name);
  }

  @Override
  public BucketSettings getBucket(String name, long timeout,
      TimeUnit timeUnit) {
    return clusterManager.getBucket(name, timeout, timeUnit);
  }

  @Override
  public Boolean hasBucket(String name) {
    return clusterManager.hasBucket(name);
  }

  @Override
  public Boolean hasBucket(String name, long timeout, TimeUnit timeUnit) {
    return clusterManager.hasBucket(name, timeout, timeUnit);
  }

  @Override
  public BucketSettings insertBucket(
      BucketSettings settings) {
    return clusterManager.insertBucket(settings);
  }

  @Override
  public BucketSettings insertBucket(
      BucketSettings settings, long timeout,
      TimeUnit timeUnit) {
    return clusterManager.insertBucket(settings, timeout, timeUnit);
  }

  @Override
  public BucketSettings updateBucket(
      BucketSettings settings) {
    return clusterManager.updateBucket(settings);
  }

  @Override
  public BucketSettings updateBucket(
      BucketSettings settings, long timeout,
      TimeUnit timeUnit) {
    return clusterManager.updateBucket(settings, timeout, timeUnit);
  }

  @Override
  public Boolean removeBucket(String name) {
    return clusterManager.removeBucket(name);
  }

  @Override
  public Boolean removeBucket(String name, long timeout, TimeUnit timeUnit) {
    return clusterManager.removeBucket(name, timeout, timeUnit);
  }

  @Override
  @Experimental
  public Boolean upsertUser(AuthDomain domain,
      String username, UserSettings settings) {
    return clusterManager.upsertUser(domain, username, settings);
  }

  @Override
  @Experimental
  public Boolean upsertUser(AuthDomain domain,
      String username, UserSettings settings, long timeout,
      TimeUnit timeUnit) {
    return clusterManager.upsertUser(domain, username, settings, timeout, timeUnit);
  }

  @Override
  @Experimental
  public Boolean removeUser(AuthDomain domain,
      String username) {
    return clusterManager.removeUser(domain, username);
  }

  @Override
  @Experimental
  public Boolean removeUser(AuthDomain domain,
      String username, long timeout, TimeUnit timeUnit) {
    return clusterManager.removeUser(domain, username, timeout, timeUnit);
  }

  @Override
  @Experimental
  public List<User> getUsers(
      AuthDomain domain) {
    return clusterManager.getUsers(domain);
  }

  @Override
  @Experimental
  public List<User> getUsers(
      AuthDomain domain, long timeout,
      TimeUnit timeUnit) {
    return clusterManager.getUsers(domain, timeout, timeUnit);
  }

  @Override
  @Experimental
  public User getUser(
      AuthDomain domain, String userid) {
    return clusterManager.getUser(domain, userid);
  }

  @Override
  @Experimental
  public User getUser(
      AuthDomain domain, String userid, long timeout,
      TimeUnit timeUnit) {
    return clusterManager.getUser(domain, userid, timeout, timeUnit);
  }

  @Override
  @Experimental
  public ClusterApiClient apiClient() {
    return clusterManager.apiClient();
  }
}
