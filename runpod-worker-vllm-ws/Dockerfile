# 기반 이미지 유지 (CUDA 12.1 + Ubuntu 22.04)
FROM nvidia/cuda:12.1.0-base-ubuntu22.04 

# 시스템 패키지 설치
RUN apt-get update -y \
    && apt-get install -y python3-pip \
    && rm -rf /var/lib/apt/lists/*

# CUDA 호환성 설정
RUN ldconfig /usr/local/cuda-12.1/compat/

# Python 종속성 설치
COPY requirements.txt /requirements.txt
RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install --upgrade -r /requirements.txt

# vLLM 및 FlashInfer 설치 (WebSocket 지원 버전에 맞게 수정)
RUN python3 -m pip install vllm==0.8.3 && \
    python3 -m pip install flashinfer -i https://flashinfer.ai/whl/cu121/torch2.3

# 워커 설정 변수
ARG MODEL_NAME=""
ARG TOKENIZER_NAME=""
ARG BASE_PATH="/runpod-volume"
ARG QUANTIZATION=""
ARG MODEL_REVISION=""
ARG TOKENIZER_REVISION=""

# 환경 변수 설정 (WebSocket 포트 추가)
ENV MODEL_NAME=$MODEL_NAME \
    MODEL_REVISION=$MODEL_REVISION \
    TOKENIZER_NAME=$TOKENIZER_NAME \
    TOKENIZER_REVISION=$TOKENIZER_REVISION \
    BASE_PATH=$BASE_PATH \
    QUANTIZATION=$QUANTIZATION \
    WEBSOCKET_PORT=8765 \
    HF_DATASETS_CACHE="${BASE_PATH}/huggingface-cache/datasets" \
    HUGGINGFACE_HUB_CACHE="${BASE_PATH}/huggingface-cache/hub" \
    HF_HOME="${BASE_PATH}/huggingface-cache/hub" \
    HF_HUB_ENABLE_HF_TRANSFER=0 

ENV PYTHONPATH="/:/vllm-workspace"

# 소스 코드 복사 (WebSocket 핸들러 포함)
COPY src /src

# 모델 다운로드 로직 유지
RUN --mount=type=secret,id=HF_TOKEN,required=false \
    if [ -f /run/secrets/HF_TOKEN ]; then \
    export HF_TOKEN=$(cat /run/secrets/HF_TOKEN); \
    fi && \
    if [ -n "$MODEL_NAME" ]; then \
    python3 /src/download_model.py; \
    fi

# WebSocket 포트 노출 (추가됨)
EXPOSE 8765

# 진입점 변경 (handler.py → rp_handler.py)
CMD ["python3", "/src/rp_handler.py"]
