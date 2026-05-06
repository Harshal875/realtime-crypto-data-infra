FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pre-generated proto files and source
COPY orderbook_pb2.py orderbook_pb2_grpc.py orderbook.proto ./
COPY server.py .

EXPOSE 50051

CMD ["python", "server.py"]
