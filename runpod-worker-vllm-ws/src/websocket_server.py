import asyncio
import websockets
import json
import logging
from utils import JobInput

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("websocket_server")

class WebSocketServer:
    def __init__(self, engine, host="0.0.0.0", port=8765):
        """
        WebSocket 서버 초기화
        
        Args:
            engine: vLLM 엔진 인스턴스
            host: 서버 호스트 주소
            port: 서버 포트
        """
        self.engine = engine
        self.host = host
        self.port = port
        self.server = None
        self.active_connections = set()
    
    async def handle_client(self, websocket, path):
        """
        클라이언트 연결 처리
        """
        self.active_connections.add(websocket)
        try:
            logger.info(f"클라이언트 연결됨: {websocket.remote_address}")
            
            async for message in websocket:
                try:
                    data = json.loads(message)
                    
                    # "shutdown" 명령 처리
                    if data.get("command") == "shutdown":
                        logger.info("종료 명령 수신됨")
                        await websocket.send(json.dumps({"status": "shutting_down"}))
                        await self.shutdown()
                        return
                    
                    # 프롬프트 처리
                    input_data = data.get("input")
                    if input_data:
                        from utils import JobInput
                        
                        # JobInput 클래스를 사용하여 입력 처리
                        job_input = JobInput(input_data)
                        logger.info(f"프롬프트 수신: {str(job_input.llm_input)[:50]}...")
                        
                        # vLLM 엔진을 사용해 응답 생성
                        results_generator = self.engine.generate(job_input)
                        async for batch in results_generator:
                            if isinstance(batch, dict) and "text" in batch:
                                await websocket.send(json.dumps({"token": batch["text"], "finished": False}))
                            elif isinstance(batch, str):
                                await websocket.send(json.dumps({"token": batch, "finished": False}))
                            else:
                                # 다른 형식의 응답 처리
                                token = str(batch)
                                await websocket.send(json.dumps({"token": token, "finished": False}))
                        
                        # 완료 메시지 전송
                        await websocket.send(json.dumps({"finished": True}))

                        
                except json.JSONDecodeError:
                    await websocket.send(json.dumps({
                        "error": "잘못된 JSON 형식입니다."
                    }))
                except Exception as e:
                    logger.error(f"메시지 처리 중 오류 발생: {str(e)}")
                    await websocket.send(json.dumps({
                        "error": f"메시지 처리 중 오류 발생: {str(e)}"
                    }))
        
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"클라이언트 연결 종료됨: {websocket.remote_address}")
        finally:
            self.active_connections.remove(websocket)
    
    async def start(self):
        """
        WebSocket 서버 시작
        """
        self.server = await websockets.serve(
            self.handle_client, 
            self.host, 
            self.port
        )
        
        logger.info(f"WebSocket 서버가 {self.host}:{self.port}에서 시작되었습니다")
        
        # 서버 실행 유지
        await self.server.wait_closed()
    
    async def shutdown(self):
        """
        서버 종료
        """
        if self.server:
            # 모든 활성 연결 종료
            for websocket in self.active_connections.copy():
                await websocket.close()
            
            self.server.close()
            await self.server.wait_closed()
            logger.info("WebSocket 서버가 종료되었습니다")
