import asyncio
import websockets
import json
import logging
from src.engine import vLLMEngine, OpenAIvLLMEngine
from src.utils import JobInput

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("WebSocketServer")

class WebSocketServer:
    def __init__(self, engine, host, port):
        self.engine = engine
        self.host = host
        self.port = port
        self.server = None
        self.active_connections = set()
        self.generation_task = None
        self.generation_complete = asyncio.Event()
        self.connection_complete = asyncio.Event()

    async def start_generation(self, job):
        """vLLM 생성 작업 시작"""
        
        self.generation_task = asyncio.create_task(
            self._handle_generation(job)
        )
        logger.info(f"추론시작")

    async def _handle_generation(self, prompt):
        """실제 텍스트 생성 처리"""
        try:
            job_input = JobInput(prompt["input"])
            results_generator = self.engine.generate(job_input)
            async for token in results_generator:
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
        try:
            # 클라이언트로부터 종료 신호 대기
            async for message in websocket:
                data = json.loads(message)
                if data.get("command") == "shutdown":
                    await websocket.send(json.dumps({
                        "status": "종료 중"
                    }))
                    logger.info(f"websocket_server 종료 요청 수신")
                    
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            self.active_connections.remove(websocket)
            logger.info(f"토큰 생성 완료")
            await self.shutdown()

    async def start(self):
        """서버 시작"""
        self.server = await websockets.serve(
            self.handle_client,
            self.host,
            self.port
        )
        logger.info(f"Websocket 서버 시작: {self.host}:{self.port}")

    async def shutdown(self):
        """그레이스풀 종료"""
        logger.info(f"websocket_server 종료 함수 실행")
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

