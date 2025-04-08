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
            json={
                "input": {
                    "messages": [
                        {"role": "system", "content": "You are Qwen, created by Alibaba Cloud. You are a helpful assistant."},
                        {"role": "user", "content": prompt}
                    ],
                    "sampling_params":{
                        "temperature": 0.7,
                        "max_tokens": 10000
                    }, 
                    "stream": True
                }
            }
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
