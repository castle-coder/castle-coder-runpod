import asyncio
import websockets
import json
import sys
import time
import requests

async def receive_stream(ws_uri):
    """WebSocket을 통해 스트리밍 응답 수신"""
    try:
        async with websockets.connect(ws_uri) as websocket:
            print("연결 성공! 응답 대기 중...")
            async for message in websocket:
                data = json.loads(message)
                
                if "error" in data:
                    print(f"\n오류 발생: {data['error']}")
                    break


                if "token" in data:
                    try:
                        print(data["token"]["choices"][0]["tokens"][0], end="", flush=True)
                    except (KeyError, IndexError):
                        pass
                else:
                    try:
                        print(data["choices"][0]["tokens"][0], end="", flush=True)
                    except (KeyError, IndexError):
                        pass
  
                # if "token" in data:
                #     print(data)
                #     print("\n")
                #     print(data["token"], end="", flush=True)
                    
                if data.get("finished", False):
                    print("\n\n[생성 완료]")
                    await websocket.send(json.dumps({"command": "shutdown"}))
                    break
    except Exception as e:
        print(f"\n연결 오류: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("사용법: python client.py <endpoint_id> <api_key> <prompt>")
        sys.exit(1)

    endpoint_id = sys.argv[1]
    api_key = sys.argv[2]
    prompt = sys.argv[3]

    # 1. 작업 시작 요청
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(
            f"https://api.runpod.ai/v2/{endpoint_id}/run",
            headers=headers,
            json={"input": {"prompt": prompt, "stream": True}}
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"요청 실패: {str(e)}")
        sys.exit(1)

    # 2. 요청 ID 추출
    try:
        request_id = response.json().get("id")
        print(f"요청 ID: {request_id}")
    except KeyError:
        print("잘못된 응답 형식")
        sys.exit(1)

    # 3. 상태 폴링 (최대 120초 대기)
    ws_uri = None
    for _ in range(120):
        try:
            status_resp = requests.get(
                f"https://api.runpod.ai/v2/{endpoint_id}/status/{request_id}",
                headers=headers
            )

            status_data = status_resp.json()
            
            if status_resp.status_code == 200:
                if status_data.get("output", {}).get("status") == "시작":
                    conn_info = status_data.get("output", {})
                    conn_ip = conn_info.get("ip")
                    conn_port = conn_info.get("port")
                    ws_uri = f"ws://{conn_ip}:{conn_port}"
                    break
   
            # 오류 상태 확인
            if status_data.get("status") == "FAILED":
                print("워커 실행 실패")
                sys.exit(1)
                
        except requests.exceptions.RequestException as e:
            print(f"상태 확인 실패: {str(e)}")
            sys.exit(1)
            
        print("워커 준비 중...")
        time.sleep(1)  # 1초 대기
    
    # 4. WebSocket 연결 실행
    if ws_uri:
        print(f"연결 시도: {ws_uri}")
        asyncio.run(receive_stream(ws_uri))
    else:
        print("연결 정보를 가져오지 못함")
        sys.exit(1)

# import asyncio
# import websockets
# import json
# import sys
# import requests

# async def connect_to_runpod_worker(endpoint_id, api_key, prompt):
#     """
#     RunPod Serverless 워커에 연결하여 LLM과 상호작용하는 클라이언트 예제
#     """
#     print("워커 활성화 중...")
    
#     # 1. 워커 활성화
#     headers = {
#         "Authorization": f"Bearer {api_key}",
#         "Content-Type": "application/json"
#     }
#     response = requests.post(
#         f"https://api.runpod.ai/v2/{endpoint_id}/run",
#         headers=headers,
#         json={"input": {"prompt": prompt}}
#     )
    
#     if response.status_code != 200:
#         print(f"워커 활성화 실패: {response.text}")
#         return
    
#     request_id = response.json().get("id")
#     print(f"워커 활성화 요청 ID: {request_id}")
    
#     # 2. 연결 정보 가져오기
#     status_response = None
#     retry_count = 0
    
#     while retry_count < 300:  # 최대 60초 대기
#         status_response = requests.get(
#             f"https://api.runpod.ai/v2/{endpoint_id}/status/{request_id}",
#             headers=headers
#         )
        
#         if status_response.status_code == 200:
#             status_data = status_response.json()
#             if status_data.get("output",{}).get("status") == "요청 대기 중":
#                 break
#             elif "progress" in status_data:
#                 progress = status_data.get("progress", {})
#                 if "ip" in progress and "port" in progress:
#                     break
        
#         print("워커 준비 중...")
#         await asyncio.sleep(1)
#         retry_count += 1
    
#     if not status_response or status_response.status_code != 200:
#         print("연결 정보를 가져오는 데 실패했습니다.")
#         return
    
#     # 3. WebSocket 연결 정보 추출
#     conn_info = None
    
#     if "output" in status_response.json():
#         conn_info = status_response.json().get("output", {})
#     else:
#         print("연결 정보를 찾을 수 없습니다.")
#         return
    
#     ws_ip = conn_info.get("ip")
#     ws_port = conn_info.get("port")
    
#     if not ws_ip or not ws_port:
#         print("IP 또는 포트 정보가 없습니다.")
#         return
    
#     # 4. WebSocket 연결 및 상호작용
#     ws_uri = f"ws://{ws_ip}:{ws_port}"
#     print(f"WebSocket 서버에 연결 중: {ws_uri}")
#     try:
#         async with websockets.connect(ws_uri) as websocket:
#             print("연결됨! 프롬프트 전송 중...")
            
#             messages = [
#                  {"role": "system", "content": "You are Qwen, created by Alibaba Cloud. You are a helpful assistant."},
#                  {"role": "user", "content": prompt}
#             ]

#             # 프롬프트 전송

#             await websocket.send(json.dumps({
#                 "input": {"prompt": messages}
#             }))
            
#             # 스트리밍 응답 처리
#             async for message in websocket:
#                 data = json.loads(message)
                
#                 if "error" in data:
#                     print(f"오류: {data['error']}")
#                     break
                
#                 if "token" in data:
#                     print(data["token"], end="", flush=True)
                
#                 if data.get("finished", False):
#                     print("\n\n생성 완료!")
#                     break
            
#             # 모든 작업이 완료되면 종료 명령 전송
#             print("서버 종료 요청 중...")
#             await websocket.send(json.dumps({"command": "shutdown"}))
            
#             # 종료 확인 응답 대기
#             try:
#                 shutdown_resp = await asyncio.wait_for(websocket.recv(), timeout=5.0)
#                 print(f"서버 응답: {shutdown_resp}")
#             except asyncio.TimeoutError:
#                 print("서버 종료 응답 대기 시간 초과")
    
#     except Exception as e:
#         print(f"WebSocket 통신 중 오류 발생: {str(e)}")

# if __name__ == "__main__":
#     if len(sys.argv) < 4:
#         print("사용법: python client.py <endpoint_id> <api_key> <prompt>")
#         sys.exit(1)
    
#     endpoint_id = sys.argv[1]
#     api_key = sys.argv[2]
#     prompt = sys.argv[3]
    
#     asyncio.run(connect_to_runpod_worker(endpoint_id, api_key, prompt))
