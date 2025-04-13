import asyncio
import websockets
import json
import logging
from src.engine import vLLMEngine, OpenAIvLLMEngine
from src.utils import JobInput
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WebSocketServer")

class WebSocketServer:
    def __init__(self, engine, host, port):
        self.engine = engine
        self.host = host
        self.port = port
        self.server = None
<<<<<<< HEAD
=======
        self.active_connections = set()
        self.generation_task = None
        self.generation_complete = asyncio.Event()
        self.connection_complete = asyncio.Event()

    async def start_generation(self, job):
        """vLLM 생성 작업 시작"""
>>>>>>> f2f21825f88adbabec7cb8f8e1c3fba2ae17c6b4
        
        self.active_connections = {}
        self.active_tasks = {}
        self.job_complete_events={}
        self.connection_complete = {}

        self.generation_task = None
        self.is_shutting_down = False

        self.connection_lock = asyncio.Lock()
        self.server_terminate = asyncio.Event()


    async def start_generation(self, job, job_id=None):
        """vLLM 생성 작업 시작"""
        if job_id is None:
            job_id = JobInput(job["input"]).request_id

        task = asyncio.create_task(
            self._handle_generation(job, job_id)
        )
        self.active_tasks[job_id] = task
        
        logger.info(f"{job_id} 추론 시작")

    async def cleanup_task(self, job_id):
        logger.info(f"{job_id} 작업 정리 시작, 남은 작업: {len(self.active_tasks)}")

        if job_id in self.active_tasks:
            del self.active_tasks[job_id]
            logger.info(f"{job_id} cleanup 완료")

        if len(self.active_tasks) == 0:
            if len(self.active_connections) == 0 and not self.is_shutting_down:
                self.is_shutting_down = True
                logger.info("모든 작업 완료, 서버 종료 시작")
                asyncio.create_task(self.shutdown())

        


        logger.info(f"{job_id} 작업 정리 완료, 남은 작업: {len(self.active_tasks)}")
        

    async def _handle_generation(self, prompt, job_id):
        """실제 텍스트 생성 처리"""
        try:
            connected_clients = [ws for ws, jid in self.active_connections.items() if jid == job_id]

            if not connected_clients:
                logger.warning(f"작업 {job_id}에 연결된 클라이언트 없음")
                if job_id in self.job_complete_events:
                    self.job_complete_events[job_id].set()
                return
            
            job_input = JobInput(prompt["input"])
<<<<<<< HEAD
            logger.info(f"JobInput처리완료, 작업id : {job_id}")
=======
>>>>>>> f2f21825f88adbabec7cb8f8e1c3fba2ae17c6b4
            results_generator = self.engine.generate(job_input)
            
            #job_id에 해당하는 client에게만 결과 전송해야함
            async for token in results_generator:
<<<<<<< HEAD
                for websocket in connected_clients:
                    if websocket in self.active_connections and self.active_connections[websocket] == job_id:
                        try:
                            await websocket.send(json.dumps({
                                "token": token,
                                "finished": False
                            }))
                            
                        except websockets.exceptions.ConnectionClosed:
                            if websocket in self.active_connections:
                                del self.active_connections[websocket]

                if not connected_clients:
                    logger.warning(f"작업 {job_id}에 연결된 클라이언트 연결 종료됨")
                    break

            # 완료 신호 전송
            for websocket in connected_clients:
                if websocket in self.active_connections and self.active_connections[websocket] == job_id:
                    try:
                        await websocket.send(json.dumps({"finished": True}))
                        logger.info(f"작업 {job_id}의 완료 신호 client에게 전송완료")
                    except websockets.exceptions.ConnectionClosed:
                        logger.info(f"완료 신호 전송 중 클라이언트 연결 종료")
                        if websocket in self.active_connections:
                            del self.active_connections[websocket]
            


                
            
        except Exception as e:
            logger.error(f"생성 오류: 작업 {job_id}: {str(e)}")
            for websocket in connected_clients:
                if websocket in self.active_connections and self.active_connections[websocket] == job_id:
                    try:
                        await websocket.send(json.dumps({"error": str(e), "finished": True}))
                    except websockets.exceptions.ConnectionClosed:
                        if websocket in self.active_connections:
                            del self.active_connections[websocket]
                        
                    logger.info(f"작업 {job_id}의 오류 신호 전송완료")
                        
            if job_id in self.job_complete_events:
                self.job_complete_events[job_id].set()
        
            


    async def handle_client(self, websocket):
        """클라이언트 연결 처리"""
        self.active_connections[websocket] = None
        job_id = None
            
=======
                # 모든 연결된 클라이언트에게 전송
                for websocket in self.active_connections.copy():
                    try:
                        await websocket.send(json.dumps({
                            "token": token,
                            "finished": False
                        }))
                    except websockets.exceptions.ConnectionClosed:
                        self.active_connections.remove(websocket)
            
            # 완료 신호 전송
            for websocket in self.active_connections.copy():
                await websocket.send(json.dumps({"finished": True}))
                logger.info(f"완료 신호 전송완료")
            
        except Exception as e:
            logger.error(f"생성 오류: {str(e)}")
            

    async def handle_client(self, websocket):
        """클라이언트 연결 처리"""
        self.active_connections.add(websocket)
        self.connection_complete.set()
>>>>>>> f2f21825f88adbabec7cb8f8e1c3fba2ae17c6b4
        try:
            # 클라이언트로부터 메시지 대기
            async for message in websocket:
                data = json.loads(message)
                if data.get("command") == "start_job":
                    if "job" in data:
                        job = data["job"]
                        if "metadata" in data and "request_id" in data["metadata"]:
                            job_id = data["metadata"]["request_id"]
                                            
                        # 웹소켓과 job_id 매핑 저장
                        self.active_connections[websocket] = job_id
                        
                        # 작업 완료 이벤트 생성
                        if job_id not in self.job_complete_events:
                            self.job_complete_events[job_id] = asyncio.Event()

                        if job_id not in self.connection_complete:
                            self.connection_complete[job_id] = asyncio.Event()
            
                        asyncio.create_task(self.start_generation(job, job_id))
                        await websocket.send(json.dumps({
                            "status": "작업 시작",
                            "job_id": job_id
                        }))

                        self.connection_complete[job_id].set()


                        logger.info(f"작업 {job_id} handle_client try문 완료")


                elif data.get("command") == "shutdown":
                    job_id = None

                    if "job" in data:
                        if "metadata" in data and "request_id" in data["metadata"]:
                            job_id = data["metadata"]["request_id"]

                    await websocket.send(json.dumps({
                        "status": "종료"
                    }))
<<<<<<< HEAD
                    break
=======
                    logger.info(f"websocket_server 종료 요청 수신")
>>>>>>> f2f21825f88adbabec7cb8f8e1c3fba2ae17c6b4
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"클라이언트 연결 종료")
            
        except json.JSONDecodeError:
            logger.error(f"잘못된 JSON 메시지 수신")
            
        except Exception as e:
            logger.error(f"클라이언트 처리 중 오류: {str(e)}")
            
        finally:
<<<<<<< HEAD
            if websocket in self.active_connections:
                job_id = self.active_connections[websocket]
                del self.active_connections[websocket]
                self.job_complete_events[job_id].set()
                asyncio.create_task(self.cleanup_task(job_id))
                logger.info(f"클라이언트 종료 완료, 남은 연결: {len(self.active_connections)}")    
=======
            self.active_connections.remove(websocket)
            logger.info(f"토큰 생성 완료")
            await self.shutdown()
>>>>>>> f2f21825f88adbabec7cb8f8e1c3fba2ae17c6b4

    async def start(self):
        """서버 시작"""
        self.server_terminate.clear()
        self.server = await websockets.serve(
            self.handle_client,
            self.host,
            self.port
        )
<<<<<<< HEAD
        self.is_shutting_down = False
        logger.info(f"서버 시작: {self.host}:{self.port}")
=======
        logger.info(f"Websocket 서버 시작: {self.host}:{self.port}")
>>>>>>> f2f21825f88adbabec7cb8f8e1c3fba2ae17c6b4

    async def shutdown(self):
        """그레이스풀 종료"""
        logger.info(f"websocket_server 종료 함수 실행")
<<<<<<< HEAD
        
        for job_id, task in self.active_tasks.copy().items():
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                if job_id in self.job_complete_events:
                    self.job_complete_events[job_id].set()
        
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            
        
        self.active_tasks.clear()
        self.active_connections.clear()
        self.job_complete_events.clear()
        self.connection_complete.clear()
        self.server_terminate.set()
        logger.info(f"Websocket 서버 종료 완료")
=======
        if self.generation_task and not self.generation_task.done():
            self.generation_task.cancel()
            try:
                await self.generation_task
            except asyncio.CancelledError:
                pass
        self.generation_complete.set()


        if self.server:
            self.server.close()
            await self.server.wait_closed()
>>>>>>> f2f21825f88adbabec7cb8f8e1c3fba2ae17c6b4

