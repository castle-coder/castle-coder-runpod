import os
import json
import requests

def download_model_weights(model_id, hf_token=None):
    """모델 가중치 다운로드 함수"""
    from huggingface_hub import snapshot_download
    
    print(f"{model_id} 가중치 다운로드 중...")
    
    cache_dir = os.environ.get("MODEL_CACHE_DIR", "/tmp/model_cache")
    os.makedirs(cache_dir, exist_ok=True)
    
    snapshot_download(
        repo_id=model_id,
        local_dir=cache_dir,
        token=hf_token
    )
    
    print(f"{model_id} 가중치 다운로드 완료")
    return cache_dir

def get_pod_info():
    """RunPod 인스턴스 정보 반환"""
    endpoint_id = os.environ.get("RUNPOD_ENDPOINT_ID")
    pod_id = os.environ.get("RUNPOD_POD_ID")
    
    return {
        "endpoint_id": endpoint_id,
        "pod_id": pod_id
    }
