FROM bitnami/spark:3.4.1

USER root
RUN apt-get update -qq && \
    apt-get install --no-install-recommends -y python3-pip && \
    apt-get clean -y && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/app

CMD ["sleep", "infinity"]