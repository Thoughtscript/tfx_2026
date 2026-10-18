FROM python:3.10-slim AS model-builder

RUN python -m pip install --no-cache-dir tensorflow-cpu==2.15.1
COPY services/tensorflow-serving/build_model.py /build_model.py
RUN python /build_model.py /models/example/1

FROM tensorflow/serving:2.15.1

COPY --from=model-builder /models/example /models/example
COPY services/tensorflow-serving/start-serving.sh /usr/local/bin/start-serving
RUN chmod +x /usr/local/bin/start-serving
ENV MODEL_NAME=example
ENV MODEL_BASE_PATH=/models/example

ENTRYPOINT ["/usr/local/bin/start-serving"]
