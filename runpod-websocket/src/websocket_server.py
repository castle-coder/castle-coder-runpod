import asyncio
import json
import websockets
import os
from src.model import LLMModel

# 모델 로드
model = None

async def handle_client(websocket, path):
    try:
        # 클라이언트 연결 설정
        print(f"클라이언트 연결: {websocket.remote_address}")
        
        # 메시지 수신
        async for message in websocket:
            try:
                data = json.loads(message)
                prompt = data.get("prompt")
                max_tokens = data.get("max_tokens", 100)
                temperature = data.get("temperature", 0.7)
                
                if not prompt:
                    await websocket.send(json.dumps({"error": "프롬프트가 필요합니다."}))
                    continue
                
                # 스트리밍 생성 시작
                await websocket.send(json.dumps({"status": "생성 시작"}))
                
                # 토큰 스트리밍
                for token in model.generate_stream(prompt, max_tokens, temperature):
                    await websocket.send(json.dumps({
                        "token": token,
                        "finished": False
                    }))
                
                # 생성 완료
                await websocket.send(json.dumps({
                    "finished": True,
                    "status": "생성 완료"
                }))
                
            except json.JSONDecodeError:
                await websocket.send(json.dumps({"error": "잘못된 JSON 형식"}))
            except Exception as e:
                await websocket.send(json.dumps({"error": str(e)}))
    
    except websockets.exceptions.ConnectionClosed:
        print(f"클라이언트 연결 종료: {websocket.remote_address}")

async def start_server(host="0.0.0.0", port=8765):
    global model
    model_id = os.environ.get("MODEL_ID", "google/gemma-2-9b-it")
    model = LLMModel(model_id)
    
    server = await websockets.serve(handle_client, host, port)
    print(f"WebSocket 서버 시작 - {host}:{port}")
    
    # 서버 IP 및 포트 정보를 RunPod에 전달
    # 이를 통해 클라이언트가 연결 정보를 알 수 있음
    return server

def get_connection_info():
    # RunPod 환경에서 공용 IP 및 포트 가져오기
    public_ip = os.environ.get("RUNPOD_PUBLIC_IP")
    public_port = os.environ.get("RUNPOD_TCP_PORT_8765")
    
    return {
        "ip": public_ip,
        "port": public_port
    }
