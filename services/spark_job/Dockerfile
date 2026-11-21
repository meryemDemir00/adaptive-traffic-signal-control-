FROM spark:3.5.8-scala2.12-java17-python3-ubuntu

USER root
RUN pip3 install --no-cache-dir clickhouse-connect==1.13.0 \
 && mkdir -p /app /tmp/.ivy \
 && chown -R spark:spark /app /tmp/.ivy
USER spark

WORKDIR /app