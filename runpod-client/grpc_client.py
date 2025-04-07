import grpc
import service_pb2
import service_pb2_grpc

def send_request():
    # RunPod 서버의 퍼블릭 IP와 포트로 연결
    target = "38.128.232.57:28284"  # 👈 너의 서버 IP:PORT로 바꿔야 해

    # gRPC 채널 생성
    with grpc.insecure_channel(target) as channel:
        stub = service_pb2_grpc.RunPodServiceStub(channel)

        # 요청 생성
        request = service_pb2.HandleRequest(
            prompt="Test from client!",
            seconds=2
        )

        print("📤 Sending request...")
        response = stub.Handle(request)
        print("📩 Response received:", response.result)

if __name__ == '__main__':
    send_request()
