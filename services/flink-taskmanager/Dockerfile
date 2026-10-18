FROM alpine:3.21 AS beam-grpc-vendor

RUN apk add --no-cache curl \
    && curl -fsSL \
      https://repo.maven.apache.org/maven2/org/apache/beam/beam-vendor-grpc-1_69_0/0.1/beam-vendor-grpc-1_69_0-0.1.jar \
      -o /beam-vendor-grpc-1_69_0-0.1.jar \
    && echo "5ce988f3020b4c0b07e68168205906c932f5871ecc5fce6f520f335f2859dbef  /beam-vendor-grpc-1_69_0-0.1.jar" \
      | sha256sum -c -

FROM flink:2.2.1-scala_2.12-java17

COPY --from=beam-grpc-vendor /beam-vendor-grpc-1_69_0-0.1.jar /opt/flink/lib/beam-vendor-grpc-1_69_0-0.1.jar
