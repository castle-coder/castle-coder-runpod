import runpod
import os
import asyncio
from src.engine import vLLMEngine, OpenAIvLLMEngine
from src.websocket_server import WebSocketServer
from src.utils import JobInput

websocket_port = 8765
exposed_port_env = f"TCP_PORT_{websocket_port}"
pod_ip = os.environ.get('RUNPOD_PUBLIC_IP', 'localhost')
pod_port = os.environ.get('RUNPOD_TCP_PORT_8765', '8765')

vllm_engine = vLLMEngine()
openai_engine = OpenAIvLLMEngine(vllm_engine)

async def handler(job):
    job_input = JobInput(job["input"])
    engine = OpenAIvLLMEngine if job_input.openai_route else vllm_engine

    websocket_server = WebSocketServer(
            engine,
            host="0.0.0.0",
            port=8765
        )
    
    # 연결 정보 공유
    asyncio.create_task(websocket_server.start())
    runpod.serverless.progress_update(job, {
        "status": "시작",
        "ip": pod_ip,
        "port": pod_port,
        "version": "Stable",
    })
    

    # 비동기 생성기 시작
    try:
        await asyncio.wait_for(websocket_server.connection_complete.wait(), timeout=5)
        await asyncio.sleep(1)  # 잠시 대기
        await websocket_server.start_generation(job)
        await websocket_server.generation_complete.wait()
        return {"status": "처리 완료"}
    except asyncio.TimeoutError:
        return {"error": "연결 시간 초과"}
    except Exception as e:
        return {"error": str(e)}
    

runpod.serverless.start(
    {
        "handler": handler,
    }
)


