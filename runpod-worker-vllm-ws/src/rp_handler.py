import runpod
import os
import asyncio
<<<<<<< HEAD
import logging
=======
>>>>>>> f2f21825f88adbabec7cb8f8e1c3fba2ae17c6b4
from src.engine import vLLMEngine, OpenAIvLLMEngine
from src.websocket_server import WebSocketServer
from src.utils import JobInput

websocket_port = 8765
exposed_port_env = f"TCP_PORT_{websocket_port}"
pod_ip = os.environ.get('RUNPOD_PUBLIC_IP', 'localhost')
pod_port = os.environ.get('RUNPOD_TCP_PORT_8765', '8765')

vllm_engine = vLLMEngine()
openai_engine = OpenAIvLLMEngine(vllm_engine)
<<<<<<< HEAD
global_websocket_server=None
server_lock = asyncio.Lock()


async def handler(job):
    logging.info(f"Handler Version 1.0.7")
    global global_websocket_server

    job_input = JobInput(job["input"])
    engine = OpenAIvLLMEngine if job_input.openai_route else vllm_engine

    #동시에 여러요청이 같은 시간에 들어와서 생길 수 있는 문제를 해결하기위해 락 설정정
    async with server_lock:
        if global_websocket_server is None:
            global_websocket_server = WebSocketServer(
                    engine,
                    host="0.0.0.0",
                    port=8765
                )
            await global_websocket_server.start()
            logging.info("WebSocket 서버 인스턴스 최초 생성 완료")
        
        if global_websocket_server.server_terminate.is_set():
            await global_websocket_server.start()
            logging.info("WebSocket 서버 인스턴스 재설정 완료")
            
=======

async def handler(job):
    job_input = JobInput(job["input"])
    engine = OpenAIvLLMEngine if job_input.openai_route else vllm_engine

    websocket_server = WebSocketServer(
            engine,
            host="0.0.0.0",
            port=8765
        )
>>>>>>> f2f21825f88adbabec7cb8f8e1c3fba2ae17c6b4
    
    # 연결 정보 공유
    runpod.serverless.progress_update(job, {
        "status": "시작",
        "ip": pod_ip,
<<<<<<< HEAD
        "port": pod_port
    })

    
    # 비동기 생성기 시작
    try:

        await global_websocket_server.server_terminate.wait()
        logging.info("WebSocket 서버 종료")


                
                
=======
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
>>>>>>> f2f21825f88adbabec7cb8f8e1c3fba2ae17c6b4
    except Exception as e:
        await global_websocket_server.shutdown()
        global_websocket_server = None
        logging.info("에러 발생으로 인한 WebSocket 서버 인스턴스 재설정 완료")
        return {"error": str(e)}
    


runpod.serverless.start(
    {
        "handler": handler,
<<<<<<< HEAD
        "concurrency_modifier": lambda x: vllm_engine.max_concurrency,
        "return_aggregate_stream": True,
    }
)
=======
    }
)


>>>>>>> f2f21825f88adbabec7cb8f8e1c3fba2ae17c6b4
