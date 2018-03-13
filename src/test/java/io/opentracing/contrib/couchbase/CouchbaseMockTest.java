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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.couchbase.client.java.Cluster;
import io.opentracing.Scope;
import io.opentracing.mock.MockSpan;
import io.opentracing.mock.MockTracer;
import io.opentracing.tag.Tags;
import io.opentracing.util.ThreadLocalScopeManager;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class CouchbaseMockTest {

  private MockTracer mockTracer = new MockTracer(new ThreadLocalScopeManager(),
      MockTracer.Propagator.TEXT_MAP);

  @Before
  public void before() {
    mockTracer.reset();
  }

  @Test
  public void test() {
    Cluster cluster = new TracingCluster(Mockito.mock(Cluster.class), mockTracer, false);
    cluster.openBucket("bucket");
    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(spans.size(), 1);

    MockSpan span = spans.get(0);
    assertTrue(span.tags().get(Tags.SPAN_KIND.getKey()).equals(Tags.SPAN_KIND_CLIENT));
    assertEquals(TracingHelper.COMPONENT_NAME, span.tags().get(Tags.COMPONENT.getKey()));
    assertEquals(span.operationName(), "openBucket");
    assertEquals(0, span.generatedErrors().size());
    assertEquals(0, span.parentId());

    assertNull(mockTracer.activeSpan());
  }

  @Test
  public void withParent() {
    Cluster cluster = new TracingCluster(Mockito.mock(Cluster.class), mockTracer, false);

    try (Scope ignore = mockTracer.buildSpan("parent").startActive(true)) {
      cluster.openBucket("bucket");
    }

    List<MockSpan> spans = mockTracer.finishedSpans();
    assertEquals(spans.size(), 2);

    MockSpan span = spans.get(0);
    assertTrue(span.tags().get(Tags.SPAN_KIND.getKey()).equals(Tags.SPAN_KIND_CLIENT));
    assertEquals(TracingHelper.COMPONENT_NAME, span.tags().get(Tags.COMPONENT.getKey()));
    assertEquals(span.operationName(), "openBucket");
    assertEquals(0, span.generatedErrors().size());
    assertEquals(spans.get(1).context().spanId(), span.parentId());

    assertNull(mockTracer.activeSpan());
  }
}
