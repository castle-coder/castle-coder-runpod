import os
import asyncio
import grpc
import runpod
import service_pb2
import logging
import service_pb2_grpc
from utils import JobInput
from engine import vLLMEngine, OpenAIvLLMEngine
import json

# 전역 변수에 gRPC 서버 인스턴스 저장
grpc_server = None

# 모델 엔진 초기화
vllm_engine = vLLMEngine()
openai_engine = OpenAIvLLMEngine(vllm_engine)

# gRPC 서비스 구현 (비동기 스트리밍)
class RunPodService(service_pb2_grpc.RunPodServiceServicer):
    def __init__(self, job):
        self.job = job
    async def Handle(self, request, context):
        logging.info(f"gRPC 연결 성립, prompt: {request.prompt}")
        # 요청에 기반해 JobInput 생성
        job_input = JobInput(self.job["input"])
        engine = openai_engine if job_input.openai_route else vllm_engine
        
        result = engine.generate(job_input)
        logging.info(1)
        logging.info(result)
        # 모델로부터 스트리밍 응답 생성 (예: async generator)
        async for token in result:
            logging.info(f"➡️ 스트리밍 중: {token}")
            message = {
                "token": token,
                "finished": False
            }
            logging.info("📤 gRPC 토큰 전송: %s", token)

            yield service_pb2.HandleResponse(result=json.dumps(message, ensure_ascii=False))
            
        message = {
            "finished": True
        }
        logging.info("📤 gRPC 종료 신호 전송")
        yield service_pb2.HandleResponse(result=json.dumps(message, ensure_ascii=False))
        

        global grpc_server
        if grpc_server:
            logging.info("✅ 스트리밍 완료 → gRPC 서버 종료")
            await grpc_server.stop(grace=1)

# gRPC 서버 시작 함수 (비동기)
async def start_grpc_server(job):
    global grpc_server
    grpc_server = grpc.aio.server()
    service_pb2_grpc.add_RunPodServiceServicer_to_server(RunPodService(job), grpc_server)
    public_ip = os.environ.get('RUNPOD_PUBLIC_IP', 'localhost')
    tcp_port = os.environ.get('RUNPOD_TCP_PORT_50051', '50051')
    listen_addr = f"0.0.0.0:50051"
    grpc_server.add_insecure_port(listen_addr)
    logging.info(f"gRPC 서버 시작: {listen_addr}")

    runpod.serverless.progress_update(job, {
        "status": "start",
        "ip": public_ip,
        "port": tcp_port
    })
    
    await grpc_server.start()
    # 서버는 백그라운드에서 실행 (서버 종료는 Handle 메서드 내에서 진행)
    asyncio.create_task(grpc_server.wait_for_termination())

# RunPod 서버리스 핸들러 (예시: gRPC 서버 실행을 보장)
async def handler(job):
    print("RunPod 서버리스 핸들러 시작")
    logging.info("RunPod 서버리스 핸들러 시작")
    logging.info("1")
    
    # gRPC 서버 시작 (여러 번 호출해도 이미 실행 중이면 영향없도록 구성)
    await start_grpc_server(job)
    
    # gRPC 서버가 종료될 때까지 기다림
    await grpc_server.wait_for_termination()
    

# RunPod 서버리스 시작
runpod.serverless.start(
    {
        "handler": handler,
        "concurrency_modifier": lambda x: vllm_engine.max_concurrency,
        "return_aggregate_stream": True,
    }
)
