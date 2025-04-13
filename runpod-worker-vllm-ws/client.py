import asyncio
import websockets
import json
import sys
import time
import requests
import uuid
import os

CONFIG_FILE = "ws_config.txt"

def read_ws_config():
    """저장된 웹소켓 설정 읽기"""
    try:
        with open(CONFIG_FILE, "r") as f:
            lines = f.read().splitlines()
            if len(lines) >= 2:
                return f"ws://{lines[0]}:{lines[1]}"
    except FileNotFoundError:
        pass
    return None

def save_ws_config(ip, port):
    """새로운 웹소켓 설정 저장"""
    with open(CONFIG_FILE, "w") as f:
        f.write(f"{ip}\n{port}")

def delete_ws_config():
    """설정 파일 삭제"""
    try:
        os.remove(CONFIG_FILE)
    except FileNotFoundError:
        pass

# 수정된 try_direct_connection() 함수
async def try_direct_connection(prompt, job_id):
    saved_ws_uri = read_ws_config()
    if not saved_ws_uri:
        return False
        
    print(f"\n[시스템] 저장된 연결 정보 사용: {saved_ws_uri}")
    
    try:
        await receive_stream(saved_ws_uri, build_job_data(prompt), job_id)
        return True
    except (websockets.exceptions.InvalidURI, 
            websockets.exceptions.InvalidHandshake,
            ConnectionRefusedError) as e:
        print(f"\n[오류] 연결 실패: {str(e)}")
        delete_ws_config()
        return False



async def main_workflow(endpoint_id, api_key, prompt):
    """주 실행 워크플로우"""
    job_id = str(uuid.uuid4())
    
    # 1차 시도: 저장된 설정으로 연결
    if read_ws_config():
        try:
            success = await try_direct_connection(prompt, job_id)
            if success:
                return
        except Exception as e:
            print(f"\n[시스템] 예상치 못한 오류: {str(e)}")
            delete_ws_config()
    
    # 2차 시도: API 통해 새 연결 생성
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(
            f"https://api.runpod.ai/v2/{endpoint_id}/run",
            headers=headers,
            json={"input": build_job_data(prompt)}
        )
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"API 요청 실패: {str(e)}")
        sys.exit(1)

    request_id = response.json().get("id")
    print(f"요청 ID: {request_id}")

    # 웹소켓 연결 정보 기다림
    ws_uri = None
    for _ in range(300):
        try:
            status_resp = requests.get(
                f"https://api.runpod.ai/v2/{endpoint_id}/status/{request_id}",
                headers=headers
            )
            status_data = status_resp.json()
            
            if status_data.get("status") == "FAILED":
                print("워커 실행 실패")
                sys.exit(1)
                
            if status_resp.status_code == 200:
                conn_info = status_data.get("output", {})
                if conn_info.get("status") == "시작":
                    conn_ip = conn_info["ip"]
                    conn_port = conn_info["port"]
                    ws_uri = f"ws://{conn_ip}:{conn_port}"
                    save_ws_config(conn_ip, conn_port)  # 새 연결 정보 저장
                    break
                
        except requests.exceptions.RequestException as e:
            print(f"상태 확인 실패: {str(e)}")
            sys.exit(1)
            
        print("워커 준비 중...")
        time.sleep(1)

    if ws_uri:
        print(f"새 연결 생성: {ws_uri}")
        await receive_stream(ws_uri, build_job_data(prompt), job_id)
    else:
        print("연결 정보 획득 실패")
        sys.exit(1)



def build_job_data(prompt):
    return {
        "input": {
            "messages": [
                {"role": "system", "content": "You are Qwen, created by Alibaba Cloud. You are a helpful assistant."},
                {"role": "user", "content": prompt}
            ],
            "sampling_params": {
                "temperature": 0.7,
                "max_tokens": 10000
            },
            "stream": True
        }
    }

async def receive_stream(ws_uri, job_data, job_id):
    """WebSocket을 통해 스트리밍 응답 수신"""
    try:
        async with websockets.connect(ws_uri) as websocket:
            print("연결 성공! 응답 대기 중...")

            await websocket.send(json.dumps({
                "command": "start_job",
                "job": job_data,
                "metadata": {
                    "request_id": job_id
                }
            }))

            init_response = await websocket.recv()
            init_data = json.loads(init_response)

            if "error" in init_data:
                print(f"\n오류 발생: {init_data['error']}")
                return
            
            
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
                    await websocket.send(json.dumps({
                        "command": "shutdown",
                        "job": job_data,
                        "metadata": {
                            "request_id": job_id
                        }
                        }))
                    break
    except websockets.exceptions.ConnectionClosedOK:
        print("\n연결이 종료되었습니다.")
        raise
    except Exception as e:
        print(f"\n연결 오류: {str(e)}")
        raise


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("사용법: python client.py <endpoint_id> <api_key> <prompt>")
        sys.exit(1)

    endpoint_id = sys.argv[1]
    api_key = sys.argv[2]
    prompt = sys.argv[3]
    
    # 최초 실행 시 설정 파일 생성
    if not os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "w") as f:
            pass
    
    asyncio.run(main_workflow(endpoint_id, api_key, prompt))

