package io.opentracing.contrib.couchbase;

import com.couchbase.client.java.transcoder.Transcoder;
import io.opentracing.Span;
import io.opentracing.Tracer;
import io.opentracing.Tracer.SpanBuilder;
import io.opentracing.noop.NoopSpan;
import io.opentracing.tag.Tags;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TracingHelper {

  private final Tracer tracer;
  private final boolean traceWithActiveSpanOnly;
  private static final String COMPONENT_NAME = "java-couchbase";

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

  public static String toString(List<?> list) {
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
}
