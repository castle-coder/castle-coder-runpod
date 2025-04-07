FROM python:3.10-slim

WORKDIR /
COPY . .
COPY requirements.txt /requirements.txt
RUN pip install -r requirements.txt
COPY rp_handler.py /

COPY . .
# gRPC 코드 생성
RUN python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. service.proto


# Start the container
CMD ["python3", "-u", "rp_handler.py"]