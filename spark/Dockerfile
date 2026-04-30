FROM apache/spark:3.5.3-scala2.12-java17-python3-r-ubuntu

USER root

RUN apt-get update && \
    apt-get install -y curl bash && \
    mkdir -p /opt/spark/.ivy2/cache /opt/spark/.ivy2/jars && \
    chown -R spark:spark /opt/spark/.ivy2 && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir psycopg2-binary
    
WORKDIR /opt/spark/work-dir

COPY jars/ /opt/spark/custom-jars/
COPY app/ /opt/spark/app/

USER spark