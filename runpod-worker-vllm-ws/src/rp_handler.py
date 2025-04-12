import runpod
import os
import asyncio
import logging
from src.engine import vLLMEngine, OpenAIvLLMEngine
from src.websocket_server import WebSocketServer
from src.utils import JobInput

websocket_port = 8765
exposed_port_env = f"TCP_PORT_{websocket_port}"
pod_ip = os.environ.get('RUNPOD_PUBLIC_IP', 'localhost')
pod_port = os.environ.get('RUNPOD_TCP_PORT_8765', '8765')
    

vllm_engine = vLLMEngine()
openai_engine = OpenAIvLLMEngine(vllm_engine)
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
            
    
    # 연결 정보 공유
    runpod.serverless.progress_update(job, {
        "status": "시작",
        "ip": pod_ip,
        "port": pod_port
    })

    
    # 비동기 생성기 시작
    try:
        # if job_id not in global_websocket_server.connection_complete:
        #     global_websocket_server.connection_complete[job_id] = asyncio.Event()

        # await global_websocket_server.connection_complete[job_id].wait()
        # logging.info(f"In rp_hander job_id: {job_id}")
        await global_websocket_server.server_terminate.wait()
        logging.info("WebSocket 서버 종료")


                
                
    except Exception as e:
        await global_websocket_server.shutdown()
        global_websocket_server = None
        logging.info("에러 발생으로 인한 WebSocket 서버 인스턴스 재설정 완료")
        return {"error": str(e)}
    


runpod.serverless.start(
    {
        "handler": handler,
        "concurrency_modifier": lambda x: vllm_engine.max_concurrency,
        "return_aggregate_stream": True,
    }
)

# import runpod
# import os
# import asyncio
# import json
# from engine import vLLMEngine, OpenAIvLLMEngine
# from websocket_server import WebSocketServer

# engine = vLLMEngine()
# OpenAIvLLMEngine = OpenAIvLLMEngine(engine)
# websocket_server = None

# async def handler(job):  # 비동기 함수로 변경
    
#     # 환경 변수 처리
#     websocket_port = 8765
#     exposed_port_env = f"TCP_PORT_{websocket_port}"
#     pod_ip = os.environ.get('RUNPOD_PUBLIC_IP', 'localhost')
#     pod_port = os.environ.get('RUNPOD_TCP_PORT_8765', '8765')
    
#     # 진행 상태 업데이트
    # runpod.serverless.progress_update(job, {  # job 객체 사용
    #     "status": "엔진 초기화 중",
    #     "ip": pod_ip,
    #     "port": pod_port
    # })

#     # vLLM 엔진 초기화
#     if not engine:
#         try:
#             engine = vLLMEngine()
#             runpod.serverless.progress_update(job, {"status": "엔진 준비 완료"})
#         except Exception as e:
#             return {"error": f"엔진 오류: {str(e)}"}

#     # WebSocket 서버 설정
#     if not websocket_server:
#         websocket_server = WebSocketServer(
#             engine, 
#             host="0.0.0.0", 
#             port=8765
#         )

#     # 비동기 서버 실행
#     server_task = asyncio.create_task(websocket_server.start())
    
#     # 백그라운드 작업 모니터링
#     while not server_task.done():
#         await asyncio.sleep(1)
#         runpod.serverless.progress_update(job, {
#             "status": "요청 대기 중",
#             "connections": len(websocket_server.active_connections),
#             "ip": pod_ip,
#             "port": pod_port
#         })

#     return {"status": "서버 종료"}

# if __name__ == "__main__":
#     runpod.serverless.start({
#         "handler": handler,
#         "concurrency_modes": {
#             "max_workers": 1  # 단일 워커 설정
#         }
#     })


# import runpod
# import os
# import asyncio
# import json
# from engine import vLLMEngine
# from websocket_server import WebSocketServer

# # 전역 변수 초기화
# engine = None
# websocket_server = None

# def handler(event):
#     """
#     RunPod 서버리스 핸들러 함수
#     """
#     engine = None
#     websocket_server=None
    
#     # 환경 변수에서 IP와 포트 정보 가져오기
#     websocket_port = 8765  # 기본 포트
#     exposed_port_env_name = f"TCP_PORT_{websocket_port}"
#     pod_ip = os.environ.get("RUNPOD_POD_IP", "127.0.0.1")
#     pod_port = int(os.environ.get('RUNPOD_TCP_PORT_8765', '8765'))
    
#     if not pod_port:
#         return {
#             "error": f"환경 변수 {exposed_port_env_name}를 찾을 수 없습니다. RunPod 설정에서 포트를 노출시켰는지 확인하세요."
#         }
    
#     # IP와 포트 정보를 progress_update로 클라이언트에게 전달
#     runpod.serverless.progress_update(event,{
#         "status": "서버 시작 중",
#         "ip": pod_ip,
#         "port": pod_port
#     })
    
    
#     # vLLM 엔진이 아직 초기화되지 않았다면 초기화
#     if engine is None:
#         try:
#             engine = vLLMEngine()  # vLLM 엔진 초기화
#             runpod.serverless.progress_update(event,{"status": "엔진 초기화 완료"})
#         except Exception as e:
#             return {
#                 "error": f"엔진 초기화 실패: {str(e)}"
#             }
    
#     # WebSocket 서버 시작
#     try:
#         if websocket_server is None:
#             websocket_server = WebSocketServer(engine, "0.0.0.0", websocket_port)
        
#         # WebSocket 서버 실행
#         runpod.serverless.progress_update(event,{"status": "WebSocket 서버 시작"})
#         asyncio.run(websocket_server.start())
        
#         return {
#             "status": "success",
#             "message": "WebSocket 서버가 종료되었습니다."
#         }
#     except Exception as e:
#         return {
#             "status": "error",
#             "message": f"WebSocket 서버 실행 중 오류 발생: {str(e)}"
#         }

# # 서버리스 모드로 시작
# if __name__ == "__main__":
#     runpod.serverless.start({"handler": handler})
