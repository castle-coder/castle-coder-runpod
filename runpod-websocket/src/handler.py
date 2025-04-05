import runpod
import asyncio
import json
import os
import src.websocket_server as websocket_server
from threading import Thread

# WebSocket 서버를 별도 스레드에서 실행
def start_websocket_server():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    server = loop.run_until_complete(websocket_server.start_server())
    
    # 연결 정보 가져오기
    connection_info = websocket_server.get_connection_info()
    print(f"WebSocket 서버 연결 정보: {connection_info}")
    
    loop.run_forever()

# RunPod 핸들러 함수
def handler(event):
    job_input = event["input"]
    
    # 요청 유형 확인
    if job_input.get("type") == "connection_info":
        # WebSocket 연결 정보 반환
        connection_info = websocket_server.get_connection_info()
        return {
            "connection_info": connection_info
        }
    elif job_input.get("type") == "shutdown":
        # 서버 종료 명령
        return {
            "status": "shutting_down"
        }
    else:
        # 일반 텍스트 생성 (REST API 경우)
        prompt = job_input.get("prompt")
        max_tokens = job_input.get("max_tokens", 100)
        temperature = job_input.get("temperature", 0.7)
        
        if not prompt:
            return {"error": "프롬프트가 필요합니다."}
        
        try:
            # 모델에서 텍스트 생성
            from src.model import LLMModel
            model_id = os.environ.get("MODEL_ID", "google/gemma-2-9b-it")
            model = LLMModel(model_id)
            
            generated_text = model.generate(prompt, max_tokens, temperature)
            
            return {
                "generated_text": generated_text
            }
        except Exception as e:
            return {"error": str(e)}

# 메인 함수
if __name__ == "__main__":
    # WebSocket 서버 시작
    websocket_thread = Thread(target=start_websocket_server)
    websocket_thread.daemon = True
    websocket_thread.start()
    
    # RunPod 서버리스 핸들러 시작
    runpod.serverless.start({"handler": handler})
