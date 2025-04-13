import grpc
import asyncio
import service_pb2
import service_pb2_grpc
import json
import sys
import requests
import time

async def receive_grpc_stream(target, prompt):
    """gRPC로 스트리밍 응답 수신"""
    async with grpc.aio.insecure_channel(target) as channel:
        stub = service_pb2_grpc.RunPodServiceStub(channel)

        request = service_pb2.HandleRequest(
            prompt=prompt,
            seconds=2  # 서버 설정에 따라 무시될 수도 있음
        )

        print("📤 Sending request (gRPC streaming)...")

        async for response in stub.Handle(request):
            try:
                data = json.loads(response.result)
                if data.get("finished"):
                    print("\n\n[생성 완료]")
                    break

                token = data.get("token", {}).get("choices", [{}])[0].get("tokens", [""])[0]
                print(token, end="", flush=True)

            except json.JSONDecodeError:
                print("⚠️ 응답 JSON 파싱 실패:", response.result)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("사용법: python grpc_client.py <endpoint_id> <api_key> <prompt>")
        sys.exit(1)

    endpoint_id = sys.argv[1]
    api_key = sys.argv[2]
    prompt = sys.argv[3]

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

    try:
        request_id = response.json().get("id")
        print(f"요청 ID: {request_id}")
    except KeyError:
        print("잘못된 응답 형식")
        sys.exit(1)

    grpc_target = None
    for _ in range(120):
        try:
            status_resp = requests.get(
                f"https://api.runpod.ai/v2/{endpoint_id}/status/{request_id}",
                headers=headers
            )
            status_data = status_resp.json()

            if status_resp.status_code == 200:
                output = status_data.get("output", {})
                if output.get("status") == "start":
                    ip = output.get("ip")
                    port = output.get("port")
                    grpc_target = f"{ip}:{port}"
                    break

            if status_data.get("status") == "FAILED":
                print("워커 실행 실패")
                sys.exit(1)

        except requests.exceptions.RequestException as e:
            print(f"상태 확인 실패: {str(e)}")
            sys.exit(1)

        print("워커 준비 중...")
        time.sleep(1)
        
    if grpc_target:
        print(f"gRPC 연결 시도: {grpc_target}")
        asyncio.run(receive_grpc_stream(grpc_target, prompt))
    else:
        print("gRPC 워커 주소를 찾지 못했습니다.")
