FROM runpod/base:0.4.0-cuda11.8.0

WORKDIR /app

# 필요한 패키지 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 소스 코드 복사
COPY src/ /app/

# TCP 포트 노출 (WebSocket용)
EXPOSE 8765

# 시작 명령어
CMD ["python", "-u", "handler.py"]
