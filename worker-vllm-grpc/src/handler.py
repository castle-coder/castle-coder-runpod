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
# 활성 클라이언트 수 관리를 위한 Lock
client_management_lock = asyncio.Lock()

# 모델 엔진 초기화
vllm_engine = vLLMEngine()
openai_engine = OpenAIvLLMEngine(vllm_engine)

# gRPC 서비스 구현 (비동기 스트리밍)
class RunPodService(service_pb2_grpc.RunPodServiceServicer):
    active_clients = 0 # 클래스 변수로 활성 클라이언트 수 추적

    def __init__(self, job):
        self.job = job
        
    async def Handle(self, request, context):
        async with client_management_lock:
            RunPodService.active_clients += 1
        logging.info(f"gRPC 클라이언트 연결됨. 현재 활성 클라이언트: {RunPodService.active_clients}, prompt: {request.prompt}")
        
        try:
            job_prompt_str = request.prompt # 문자열로 받음
            job_prompt_dict = json.loads(job_prompt_str) # JSON 문자열을 딕셔너리로 파싱
            job_input = JobInput(job_prompt_dict["input"]) # 파싱된 딕셔너리 사용
            engine = openai_engine if job_input.openai_route else vllm_engine
            
            result = engine.generate(job_input)
            generated_text_parts = []  # 생성된 텍스트만 저장할 리스트
            
            async for token in result:
                # 토큰에서 실제 텍스트 추출 (클라이언트와 동일한 방식)
                try:
                    actual_text = token.get("choices", [{}])[0].get("tokens", [""])[0]
                    generated_text_parts.append(actual_text)
                except (KeyError, IndexError, AttributeError):
                    # 토큰 구조가 예상과 다를 경우 전체 토큰을 문자열로 변환
                    generated_text_parts.append(str(token))
                
                message = {
                    "token": token,
                    "finished": False
                }
                yield service_pb2.HandleResponse(result=json.dumps(message, ensure_ascii=False))
            
            # 전체 생성된 텍스트 로깅
            full_generated_text = "".join(generated_text_parts)
            logging.info(f"{job_input.request_id}의 답변 완료. 생성된 텍스트: {full_generated_text}")

            message = {
                "finished": True
            }
            logging.info("📤 gRPC 종료 신호 전송")
            yield service_pb2.HandleResponse(result=json.dumps(message, ensure_ascii=False))

        except grpc.aio.AioRpcError as e:
            logging.error(f"gRPC 스트리밍 중 오류 발생: {e.code()}", exc_info=True)
            # 클라이언트가 갑자기 연결을 끊는 경우 (CANCELLED, UNAVAILABLE 등)
            if e.code() == grpc.StatusCode.CANCELLED or e.code() == grpc.StatusCode.UNAVAILABLE:
                logging.info(f"클라이언트 연결 비정상 종료 감지: {e.code()}")
        except Exception as e:
            logging.error(f"Handle 메서드 내 일반 오류: {e}", exc_info=True)
            error_message = {
                "error": str(e),
                "finished": True 
            }
            try:
                yield service_pb2.HandleResponse(result=json.dumps(error_message, ensure_ascii=False))
            except Exception as yield_e:
                logging.error(f"오류 메시지 전송 실패: {yield_e}", exc_info=True)
        finally:
            async with client_management_lock:
                RunPodService.active_clients -= 1
                current_active_clients = RunPodService.active_clients
            logging.info(f"gRPC 클라이언트 연결 종료. 현재 활성 클라이언트: {current_active_clients}")
            
            if current_active_clients == 0:
                global grpc_server
                if grpc_server:
                    logging.info("✅ 모든 클라이언트 연결 종료됨 → gRPC 서버 종료 예약")
                    asyncio.create_task(self.schedule_server_stop())

    async def schedule_server_stop(self):
        await asyncio.sleep(0.5) 
        global grpc_server
        async with client_management_lock: # Lock을 사용하여 active_clients 접근 동기화
            if grpc_server and RunPodService.active_clients == 0:
                logging.info("✅ 예약된 gRPC 서버 종료 실행")
                await grpc_server.stop(grace=1) 
            elif grpc_server and RunPodService.active_clients > 0:
                logging.info(f"ℹ️ 서버 종료 예약되었으나, 새 클라이언트({RunPodService.active_clients}명)가 연결되어 종료 취소.")


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
    # 서버는 백그라운드에서 실행 (서버 종료는 Handle 메서드 내 조건 또는 핸들러 종료 시 처리)
    # handler 함수에서 wait_for_termination을 호출하므로 여기서는 create_task 불필요.

# RunPod 서버리스 핸들러 (예시: gRPC 서버 실행을 보장)
async def handler(job):
    logging.info("RunPod 서버리스 핸들러 시작")
    logging.info("1")
    
    
    # gRPC 서버 시작 (여러 번 호출해도 이미 실행 중이면 영향없도록 구성)
    await start_grpc_server(job)
    logging.info("gRPC 서버 시작")
    
    # gRPC 서버가 종료될 때까지 기다림
    # RunPodService 내에서 active_clients == 0이 되면 서버가 stop()되고, 이 await가 풀림.
    global grpc_server
    if grpc_server: # 서버가 성공적으로 시작되었는지 확인
        await grpc_server.wait_for_termination()
        grpc_server = None  # 서버가 종료되었으므로 None으로 초기화
        logging.info("RunPod 핸들러: gRPC 서버 종료됨.")

    else:
        logging.error("RunPod 핸들러: gRPC 서버가 초기화되지 않았거나 시작에 실패했습니다.")
    
    logging.info("RunPod 서버리스 핸들러 종료")


# RunPod 서버리스 시작
runpod.serverless.start(
    {
        "handler": handler,
        "concurrency_modifier": lambda x: vllm_engine.max_concurrency,
        "return_aggregate_stream": True,
    }
)
