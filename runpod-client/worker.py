import requests

# 엔드포인트 ID로 RunPod Serverless 워커 시작
endpoint_id = "otbo7xt39plpzh"
api_key = "rpa_JXPAS3TMYRYAT0H0ZVXSGENZ3BIET1EMOBKUCJMP0yngu7"
url = f"https://api.runpod.ai/v2/{endpoint_id}/run"

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {api_key}"
}

payload = {
    "input": {
        "type": "connection_info"
    }
}

response = requests.post(url, headers=headers, json=payload)
result = response.json()

print(result)

# WebSocket 연결 정보 추출
connection_info = result.get("output", {}).get("connection_info", {})
websocket_ip = connection_info.get("ip")
websocket_port = connection_info.get("port")

print(f"WebSocket 서버 연결 정보: ws://{websocket_ip}:{websocket_port}")
