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

import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.Tags;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TracingHelper {

  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;
  static final String COMPONENT_NAME = "java-couchbase";

  public TracingHelper(Tracer tracer, boolean traceWithActiveSpanOnly) {
    this.tracer = tracer;
    this.traceWithActiveSpanOnly = traceWithActiveSpanOnly;
  }

  public Span buildSpan(String operationName) {
    if (traceWithActiveSpanOnly && tracer.activeSpan() == null) {
      return NoopSpan.INSTANCE;
    } else {
      return builder(operationName).start();
    }
  }

  private SpanBuilder builder(String operationName) {
    return tracer.buildSpan(operationName)
        .withTag(Tags.COMPONENT.getKey(), COMPONENT_NAME)
        .withTag(Tags.SPAN_KIND.getKey(), Tags.SPAN_KIND_CLIENT)
        .withTag(Tags.DB_TYPE.getKey(), "couchbase");
  }

  public static void onError(Throwable throwable, Span span) {
    Tags.ERROR.set(span, Boolean.TRUE);

    if (throwable != null) {
      span.log(errorLogs(throwable));
    }
  }

  private static Map<String, Object> errorLogs(Throwable throwable) {
    Map<String, Object> errorLogs = new HashMap<>(2);
    errorLogs.put("event", Tags.ERROR.getKey());
    errorLogs.put("error.object", throwable);
    return errorLogs;
  }

  public static String nullable(Object object) {
    return object == null ? "null" : object.toString();
  }

  public static String nullableClass(Object object) {
    return object == null ? "null" : object.getClass().getName();
  }

  public static String toStringClass(List<?> list) {
    if (list == null) {
      return "null";
    }
    StringBuilder builder = new StringBuilder();
    for (int i = 0; i < list.size(); i++) {
      if (i == 0) {
        builder.append(nullableClass(list.get(i)));
      } else {
        builder.append(", ").append(nullableClass(list.get(i)));
      }
    }

    return builder.toString();
  }

  public static String toString(Collection<?> collection) {
    if (collection == null) {
      return "null";
    }
    boolean first = true;
    StringBuilder builder = new StringBuilder();
    for (Object element : collection) {
      if (first) {
        builder.append(nullable(element));
        first = false;
      } else {
        builder.append(", ").append(nullable(element));
      }
    }

    return builder.toString();
  }

  public Tracer getTracer() {
    return tracer;
  }
}
