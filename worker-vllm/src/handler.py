import os
import runpod
import asyncio
import websockets
import threading
import json
from utils import JobInput
from engine import vLLMEngine, OpenAIvLLMEngine

# vLLM 엔진 인스턴스 생성
vllm_engine = vLLMEngine()
openai_vllm_engine = OpenAIvLLMEngine(vllm_engine)

# 전역 변수: 현재 모델 스트리밍 생성기를 저장합니다.
streaming_results = None

# WebSocket 핸들러: 클라이언트가 연결되면 모델 응답을 토큰 단위로 전송합니다.
async def websocket_handler(websocket, path):
    global streaming_results
    print("✅ WebSocket client connected")
    try:
        async for token in streaming_results:
            await websocket.send(token)
        await websocket.send("[DONE]")
    except Exception as e:
        error_msg = json.dumps({"error": str(e)})
        await websocket.send(error_msg)
    print("👋 WebSocket client disconnected")

# WebSocket 서버를 별도 쓰레드에서 실행하는 함수
def start_websocket_server():
    host = "0.0.0.0"
    port = 8765
    # 새 이벤트 루프를 만들어 실행
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    start_server = websockets.serve(websocket_handler, host, port)
    loop.run_until_complete(start_server)
    print(f"🚀 WebSocket server started on ws://{host}:{port}")
    loop.run_forever()

# WebSocket 서버를 백그라운드 쓰레드에서 시작 (데몬 모드)
ws_thread = threading.Thread(target=start_websocket_server, daemon=True)
ws_thread.start()

# RunPod Serverless HTTP 핸들러
async def handler(job):
    global streaming_results
    # job["input"]를 파싱하여 JobInput 객체 생성
    job_input = JobInput(job["input"])
    # openai_route 여부에 따라 사용할 엔진 선택
    engine = openai_vllm_engine if job_input.openai_route else vllm_engine
    # 모델 추론을 시작하여 스트리밍 생성기를 전역 변수에 저장합니다.
    streaming_results = engine.generate(job_input)
    
    # HTTP 응답으로 WebSocket 접속 정보를 반환합니다.
    ip = os.environ.get('RUNPOD_PUBLIC_IP', 'localhost')
    port = int(os.environ.get('RUNPOD_TCP_PORT_8765', '8765'))
    
    yield {"ip": ip, "port": port}

# RunPod Serverless 시작
runpod.serverless.start({
    "handler": handler,
    "concurrency_modifier": lambda x: vllm_engine.max_concurrency,
    "return_aggregate_stream": True,
})
