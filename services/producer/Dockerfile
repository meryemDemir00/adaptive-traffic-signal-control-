FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libx11-6 \
    libxext6 \
    libxrender1 \
    libxft2 \
    libgl1 \
    libglu1-mesa \
    libgomp1 \
    libatomic1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY services/producer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
    && ! ldd /usr/local/lib/python3.11/site-packages/sumo/bin/sumo | grep "not found"

ENV SUMO_HOME=/usr/local/lib/python3.11/site-packages/sumo

COPY config ./config
COPY sumo ./sumo
COPY services/producer ./services/producer

ENTRYPOINT ["python", "-u", "services/producer/main.py"]