[![Build Status][ci-img]][ci] [![Released Version][maven-img]][maven]

# OpenTracing Couchbase Client Instrumentation
OpenTracing instrumentation for Couchbase Client.

## Installation

pom.xml
```xml
<dependency>
    <groupId>io.opentracing.contrib</groupId>
    <artifactId>opentracing-couchbase-client</artifactId>
    <version>VERSION</version>
</dependency>
```

## Usage


```java
// Instantiate tracer
Tracer tracer = ...

// Decorate Couchbase Cluster with TracingCluster
Cluster cluster = new TracingCluster(CouchbaseCluster.create("localhost"), tracer);

// Open bucket
Bucket bucket = cluster.openBucket("bucketName");
```

### Async API
For async API [RxJava instrumentation](https://github.com/opentracing-contrib/java-rxjava) is used

#### Subscriber
```java
// Decorate RxJava Subscriber  with TracingSubscriber
Subscriber<JsonDocument> subscriber = ...

Subscriber<JsonDocument> tracingSubscriber = new TracingSubscriber<>(subscriber, "get", tracer);

bucket
    .async()
    .get("id")
    .subscribe(tracingSubscriber);
```

#### Action
```java
// Decorate RxJava Action with TracingActionSubscriber
Action1<JsonDocument> onNext = new Action1<JsonDocument>() {
  @Override
  public void call(JsonDocument document) {
    System.out.println(document.id());
  }
};

TracingActionSubscriber<JsonDocument> tracingActionSubscriber = new TracingActionSubscriber<>(
    onNext, "get", tracer);

bucket
    .async()
    .get("id")
    .subscribe(tracingActionSubscriber);

```

#### Observer
```java
// Decorate RxJava Observer with TracingObserverSubscriber
Observer<JsonDocument> observer = ...

TracingObserverSubscriber<Integer> tracingSubscriber = new TracingObserverSubscriber(observer, 
    "get", tracer);

bucket
    .async()
    .get("id")
    .subscribe(tracingSubscriber);
```



[ci-img]: https://travis-ci.org/opentracing-contrib/java-couchbase-client.svg?branch=master
[ci]: https://travis-ci.org/opentracing-contrib/java-couchbase-client
[maven-img]: https://img.shields.io/maven-central/v/io.opentracing.contrib/opentracing-couchbase-client.svg
[maven]: http://search.maven.org/#search%7Cga%7C1%7Copentracing-couchbase-client
