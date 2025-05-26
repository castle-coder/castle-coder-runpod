import grpc
import asyncio
import service_pb2
import service_pb2_grpc
import json
import sys
import requests
import time
import os

async def receive_grpc_stream(target, prompt):
    """gRPC로 스트리밍 응답 수신"""
    async with grpc.aio.insecure_channel(target) as channel:
        stub = service_pb2_grpc.RunPodServiceStub(channel)

        # build_job_data의 결과를 JSON 문자열로 변환
        prompt_json_str = json.dumps(build_job_data(prompt))

        request = service_pb2.HandleRequest(
            prompt=prompt_json_str,  # JSON 문자열을 prompt 필드에 할당
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
            except grpc.aio.AioRpcError as e:
                print(f"gRPC 연결 실패: {target}, 오류: {e}")
                raise

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

    grpc_server_file = "grpc_server.txt"
    grpc_target = None
    streaming_successful = False # 스트리밍 성공 여부 플래그

    # 1. 같은 폴더에 있는 기존에 저장된 grpc_server.txt에서 IP,포트를 파싱해서 접근 시도
    if os.path.exists(grpc_server_file):
        try:
            with open(grpc_server_file, "r") as f:
                content = f.read().strip()
                if ":" in content: # 간단한 IP:PORT 형식 검사
                    print(f"저장된 gRPC 서버 정보 사용 시도: {content}")
                    try:
                        asyncio.run(receive_grpc_stream(content, prompt))
                        grpc_target = content # 성공 시 grpc_target 설정
                        streaming_successful = True # 스트리밍 성공
                    except Exception as e: 
                        print(f"저장된 gRPC 서버({content}) 연결 실패: {e}")
                        print(f"{grpc_server_file} 내용 제거 중...")
                        with open(grpc_server_file, "w") as f_clear:
                            f_clear.write("") 
                        grpc_target = None 
                else:
                    print(f"{grpc_server_file} 내용이 유효하지 않아 제거합니다.")
                    with open(grpc_server_file, "w") as f_clear:
                        f_clear.write("")
        except Exception as e:
            print(f"{grpc_server_file} 처리 중 오류 발생: {e}")
            if os.path.exists(grpc_server_file):
                try:
                    with open(grpc_server_file, "w") as f_clear:
                        f_clear.write("")
                except Exception as e_clear:
                    print(f"{grpc_server_file} 내용 제거 중 오류 발생: {e_clear}")
            grpc_target = None

    # 2. 스트리밍이 성공적으로 완료되었으면 여기서 프로그램 종료
    if streaming_successful:
        sys.exit(0)

    # 3. 기존 로직 실행 (grpc_target을 아직 찾지 못했거나, 첫 시도가 실패한 경우)
    if not grpc_target: # grpc_target이 None이면 API 호출 로직 실행
        print("기존 로직 실행하여 새 gRPC 서버 정보 가져오기...")
        try:
            response = requests.post(
                f"https://api.runpod.ai/v2/{endpoint_id}/run",
                headers=headers,
                json={"input": {"messages": [{"role": "user", "content": prompt}],"sampling_params": {"max_tokens": 8192}, "stream": True}}
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"RunPod API 요청 실패: {str(e)}")
            sys.exit(1)

        try:
            request_id = response.json().get("id")
            print(f"요청 ID: {request_id}")
        except (KeyError, json.JSONDecodeError): 
            print("RunPod API 응답에서 ID를 찾을 수 없거나 잘못된 형식입니다.")
            sys.exit(1)

        new_grpc_target_from_api = None
        for i in range(500): 
            try:
                status_resp = requests.get(
                    f"https://api.runpod.ai/v2/{endpoint_id}/status/{request_id}",
                    headers=headers
                )
                status_resp.raise_for_status() 
                status_data = status_resp.json()

                if status_data.get("status") == "COMPLETED" and "output" in status_data:
                     output = status_data.get("output", {})
                     if output and "ip" in output and "port" in output:
                        ip = output.get("ip")
                        port = output.get("port")
                        new_grpc_target_from_api = f"{ip}:{port}"
                        print(f"새 gRPC 서버 정보 확인 (COMPLETED): {new_grpc_target_from_api}")
                        break
                elif status_data.get("status") == "IN_PROGRESS" or status_data.get("status") == "IN_QUEUE":
                    print(f"워커 준비 중... ({i+1}/500) 현재 상태: {status_data.get('status')}")
                
                elif status_data.get("status") == "FAILED":
                    print("워커 실행 실패")
                    error_details = status_data.get("error")
                    if error_details:
                        print(f"실패 원인: {error_details}")
                    sys.exit(1)
                
                if "output" in status_data and status_data["output"] is not None:
                    output_status = status_data["output"].get("status")
                    if output_status == "start":
                        ip = status_data["output"].get("ip")
                        port = status_data["output"].get("port")
                        if ip and port:
                            new_grpc_target_from_api = f"{ip}:{port}"
                            print(f"새 gRPC 서버 정보 확인 (output.status=start): {new_grpc_target_from_api}")
                            break
            except requests.exceptions.RequestException as e:
                print(f"상태 확인 실패: {str(e)}")
                if i == 499: 
                     sys.exit(1)
            except json.JSONDecodeError:
                print(f"상태 확인 응답 JSON 파싱 실패 ({i+1}/500)")
                if i == 499:
                    sys.exit(1)
            time.sleep(1) 
        
        if new_grpc_target_from_api:
            grpc_target = new_grpc_target_from_api
            try:
                with open(grpc_server_file, "w") as f:
                    f.write(grpc_target)
                print(f"새 gRPC 서버 정보 저장: {grpc_server_file} -> {grpc_target}")
            except Exception as e:
                print(f"{grpc_server_file} 저장 실패: {e}")
            
            # API를 통해 얻은 grpc_target으로 스트리밍 실행
            print(f"API로 얻은 gRPC 서버로 연결 시도: {grpc_target}")
            try:
                asyncio.run(receive_grpc_stream(grpc_target, prompt))
                streaming_successful = True # 스트리밍 성공
            except Exception as e:
                print(f"API로 얻은 gRPC 서버({grpc_target}) 스트림 실행 중 오류 발생. 타입: {type(e)}, 내용: {e!r}")
                if os.path.exists(grpc_server_file):
                    try:
                        current_content_in_file = ""
                        with open(grpc_server_file, "r") as f_read_check:
                            current_content_in_file = f_read_check.read().strip()
                        if current_content_in_file == grpc_target: 
                            print(f"API로 얻은 gRPC 서버 연결 실패로 {grpc_server_file} 내용 제거 중...")
                            with open(grpc_server_file, "w") as f_clear_final:
                                f_clear_final.write("")
                    except Exception as e_clear_final:
                        print(f"{grpc_server_file} 최종 정리 중 오류: {e_clear_final}")
        else:
            print("RunPod API를 통해 gRPC 워커 주소를 찾지 못했습니다.")
            sys.exit(1)
            
    if not streaming_successful and not grpc_target: # 모든 시도 후에도 target 없고 성공도 못했을 때
        print("gRPC 워커 주소를 찾지 못했고, 스트리밍에 성공하지 못했습니다.")
        sys.exit(1)
    elif not streaming_successful and grpc_target: # target은 있었으나 (예: 파일에서 읽었으나 첫 시도 실패 후 API 호출 안함) 실패한 경우
        print(f"gRPC 서버({grpc_target}) 연결 또는 스트리밍에 최종 실패했습니다.")
        sys.exit(1)
    # 성공적으로 스트리밍이 완료된 경우 (streaming_successful = True) 프로그램은 이미 위에서 sys.exit(0)으로 종료됨
