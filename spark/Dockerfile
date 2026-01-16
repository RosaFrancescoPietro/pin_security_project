FROM apache/spark:3.4.2

USER root

# Imposta HOME esplicitamente
ENV HOME=/home/spark

# Crea directory necessarie
RUN mkdir -p /home/spark/.ivy2/cache \
    && mkdir -p /opt/spark/work-dir \
    && chown -R spark:spark /home/spark /opt/spark

USER spark

WORKDIR /opt/spark/work-dir

COPY processor.py .
