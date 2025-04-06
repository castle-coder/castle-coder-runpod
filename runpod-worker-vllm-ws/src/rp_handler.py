import runpod
import os
import asyncio
from src.engine import vLLMEngine
from src.websocket_server import WebSocketServer
from src.utils import JobInput, random_uuid

websocket_port = 8765
exposed_port_env = f"TCP_PORT_{websocket_port}"
pod_ip = os.environ.get('RUNPOD_PUBLIC_IP', 'localhost')
pod_port = os.environ.get('RUNPOD_TCP_PORT_8765', '8765')
    

# 전역 변수 초기화
engine = vLLMEngine()
websocket_server = WebSocketServer(
            engine,
            host="0.0.0.0",
            port=8765
        )


async def handler(job):
    # 연결 정보 공유
    asyncio.create_task(websocket_server.start())
    runpod.serverless.progress_update(job, {
        "status": "시작",
        "ip": pod_ip,
        "port": pod_port,
        "version":"테스트2"
    })

    await asyncio.sleep(2)  # 잠시 대기

    # 비동기 생성기 시작
    try:
        await websocket_server.start_generation(job)
        await websocket_server.generation_complete.wait()
        return {"status": "처리 완료"}
    except Exception as e:
        return {"error": str(e)}
    

runpod.serverless.start(
    {
        "handler": handler,
        "return_aggregate_stream": True
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
