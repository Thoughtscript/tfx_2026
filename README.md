# tfx_2026

[![Apache Beam 2.77.0](https://img.shields.io/badge/Apache%20Beam-2.77.0-F9A800?logo=apachebeam&logoColor=white)](https://beam.apache.org/documentation/runners/flink/)
[![Apache Flink 2.2.1](https://img.shields.io/badge/Apache%20Flink-2.2.1-E6526F?logo=apacheflink&logoColor=white)](https://nightlies.apache.org/flink/flink-docs-release-2.2/)
[![Python SDK 3.11](https://img.shields.io/badge/Python%20SDK-3.11-3776AB?logo=python&logoColor=white)](https://docs.python.org/3.11/)
[![Java 17](https://img.shields.io/badge/Java-17-ED8B00?logo=openjdk&logoColor=white)](https://docs.oracle.com/en/java/javase/17/)
[![Apache Airflow 2.9.3](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/docs/apache-airflow/2.9.3/)
[![TensorFlow Serving 2.15.1](https://img.shields.io/badge/TensorFlow%20Serving-2.15.1-FF6F00?logo=tensorflow&logoColor=white)](https://www.tensorflow.org/tfx/serving)
[![PostgreSQL 16](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/docs/16/)

A simple Apache Airflow (with backing Postgres), Beam, Flink, and Tensorflow TFX example.

> This is an AI-assisted (AI-first development) example.

## Commands

Launch and cleanup
```bash
docker compose up -d

# wipe all anon volumes
docker system prune --volumes
```

Useful to test features with:
```bash
bash query-predications.sh

curl --fail-with-body \
  -X POST http://localhost:8501/v1/models/example:predict \
  -H 'Content-Type: application/json' \
  -d '{"signature_name":"serving_default","instances":[{"features":[2.0]}]}'
```

## Endpoints

* http://localhost:8501/v1/models/example
* http://localhost:8501/v1/models/example:predict
* http://localhost:8080/dags/beam_flink_tensorflow_postgres <- execute pipeline in here with creds: `airflow`, `airflow`

## java.lang.NoClassDefFoundError

It took over an hour to get AI to fix this issue:
```
flink-taskmanager-1   | Oct 18, 2026 3:55:48 AM org.apache.beam.vendor.grpc.v1p69p0.io.netty.util.concurrent.SingleThreadEventExecutor$4 run
flink-taskmanager-1   | WARNING: Unexpected exception from an event executor: 
flink-taskmanager-1   | java.lang.NoClassDefFoundError: org/apache/beam/vendor/grpc/v1p69p0/io/netty/util/collection/IntObjectHashMap$2
flink-taskmanager-1   | 	at org.apache.beam.vendor.grpc.v1p69p0.io.netty.util.collection.IntObjectHashMap.values(IntObjectHashMap.java:221)
flink-taskmanager-1   | 	at org.apache.beam.vendor.grpc.v1p69p0.io.netty.channel.epoll.EpollEventLoop.closeAll(EpollEventLoop.java:454)
flink-taskmanager-1   | 	at org.apache.beam.vendor.grpc.v1p69p0.io.netty.channel.epoll.EpollEventLoop.run(EpollEventLoop.java:422)
flink-taskmanager-1   | 	at org.apache.beam.vendor.grpc.v1p69p0.io.netty.util.concurrent.SingleThreadEventExecutor$4.run(SingleThreadEventExecutor.java:994)
flink-taskmanager-1   | 	at org.apache.beam.vendor.grpc.v1p69p0.io.netty.util.internal.ThreadExecutorMap$2.run(ThreadExecutorMap.java:74)
flink-taskmanager-1   | 	at org.apache.beam.vendor.grpc.v1p69p0.io.netty.util.concurrent.FastThreadLocalRunnable.run(FastThreadLocalRunnable.java:30)
flink-taskmanager-1   | 	at java.base/java.lang.Thread.run(Unknown Source)
flink-taskmanager-1   | Caused by: java.lang.ClassNotFoundException: org.apache.beam.vendor.grpc.v1p69p0.io.netty.util.collection.IntObjectHashMap$2
flink-taskmanager-1   | 	... 7 more
```

I searched around on Google and found several recommendations after being rebuffed by AI repeatedly and finally got it to comply:

"The TaskManager stack traces name classes in Beam's relocated gRPC/Netty
namespace `org.apache.beam.vendor.grpc.v1p69p0`. The TaskManager image now
places Beam's matching `beam-vendor-grpc-1_69_0` JAR on Flink's parent
classpath, and `classloader.parent-first-patterns.additional` makes this
package resolve from that stable classpath rather than the per-job classloader.
The JAR download is pinned by SHA-256 in the TaskManager Dockerfile.

Two consecutive end-to-end DAG runs with this configuration succeeded and
produced no `call already closed`, `IntObjectHashMap$2`, or
`GlobalEventExecutor$2` errors in the TaskManager logs; all three prediction
rows were present in Postgres. This is encouraging validation, not proof that
every workload or deployment will be free of the race. The change keeps
Flink's classloader leak checking enabled and installs no log filter. The
suggested `classloader.parent-first-check.enabled` and `classloader.check-leaks`
keys are not used; do not disable classloader safety checks to hide cleanup
errors."

## java.lang.IllegalStateException: call already closed

There's apparently a [known issue](https://issues.apache.org/jira/browse/BEAM-14354?page=com.atlassian.jira.plugin.system.issuetabpanels%3Aall-tabpanel) surrounding `call already closed`. The runs complete successfully and correctly log their results into Postgres. According to the Jira ticket above, it's considered an innocuous issue that can be ignored. I've left the logs this way:

```bash
flink-taskmanager-1   | java.lang.IllegalStateException: call already closed
flink-taskmanager-1   | 	at org.apache.beam.vendor.grpc.v1p69p0.com.google.common.base.Preconditions.checkState(Preconditions.java:513) ~[beam-vendor-grpc-1_69_0-0.1.jar:?]
flink-taskmanager-1   | 	at org.apache.beam.vendor.grpc.v1p69p0.io.grpc.internal.ServerCallImpl.closeInternal(ServerCallImpl.java:218) ~[beam-vendor-grpc-1_69_0-0.1.jar:?]
flink-taskmanager-1   | 	at org.apache.beam.vendor.grpc.v1p69p0.io.grpc.internal.ServerCallImpl.close(ServerCallImpl.java:213) ~[beam-vendor-grpc-1_69_0-0.1.jar:?]
flink-taskmanager-1   | 	at org.apache.beam.vendor.grpc.v1p69p0.io.grpc.stub.ServerCalls$ServerCallStreamObserverImpl.onCompleted(ServerCalls.java:395) ~[beam-vendor-grpc-1_69_0-0.1.jar:?]
flink-taskmanager-1   | 	at org.apache.beam.runners.fnexecution.state.GrpcStateService.close(GrpcStateService.java:63) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.sdk.fn.server.GrpcFnServer.close(GrpcFnServer.java:158) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory$WrappedSdkHarnessClient.close(DefaultJobBundleFactory.java:650) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory$WrappedSdkHarnessClient.unref(DefaultJobBundleFactory.java:666) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory.lambda$createEnvironmentCaches$3(DefaultJobBundleFactory.java:214) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LocalCache.processPendingNotifications(LocalCache.java:1850) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LocalCache$Segment.runUnlockedCleanup(LocalCache.java:3503) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LocalCache$Segment.postWriteCleanup(LocalCache.java:3479) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LocalCache$Segment.clear(LocalCache.java:3252) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LocalCache.clear(LocalCache.java:4343) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.cache.LocalCache$LocalManualCache.invalidateAll(LocalCache.java:4960) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory.close(DefaultJobBundleFactory.java:327) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.runners.fnexecution.control.DefaultExecutableStageContext.close(DefaultExecutableStageContext.java:43) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.runners.fnexecution.control.ReferenceCountingExecutableStageContextFactory$WrappedContext.closeActual(ReferenceCountingExecutableStageContextFactory.java:219) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.runners.fnexecution.control.ReferenceCountingExecutableStageContextFactory.release(ReferenceCountingExecutableStageContextFactory.java:182) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
flink-taskmanager-1   | 	at org.apache.beam.runners.fnexecution.control.ReferenceCountingExecutableStageContextFactory.scheduleRelease(ReferenceCountingExecutableStageContextFactory.java:136) ~[blob_p-007c78deb7f0dad2691e1940c849f772abe10fee-b5efc2a738e64c7b9458cb8200c9ffe0:?]
```

## Screenshot

The logs are pretty dense and it takes a moment for everything to spin up. 

As such, I've added a [screenshot](./Screenshot%202026-10-18%20at%2012.09.20 AM.png) to quickly grok what the expected state should be after running the Airflow DAG.

## Resources and Links

1. https://www.tensorflow.org/tfx
1. https://issues.apache.org/jira/browse/BEAM-14354?page=com.atlassian.jira.plugin.system.issuetabpanels%3Aall-tabpanel