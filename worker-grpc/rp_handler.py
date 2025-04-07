import grpc
import asyncio
import os
import runpod
import service_pb2
import service_pb2_grpc

# 비동기 큐 생성
response_queue = asyncio.Queue()

# gRPC 서비스 구현 (비동기)
class RunPodService(service_pb2_grpc.RunPodServiceServicer):
    async def Handle(self, request, context):
        prompt = request.prompt
        seconds = request.seconds
        print(f"📥 Received prompt: {prompt} (sleeping {seconds}s)")
        await asyncio.sleep(seconds)  # 비동기 대기
        result = f"Processed: {prompt}"
        await response_queue.put(result)  # 결과를 비동기 큐에 전달
        return service_pb2.HandleResponse(result=result)

# 비동기 gRPC 서버 실행 함수
async def start_grpc_server():
    try:
        server = grpc.aio.server()
        service_pb2_grpc.add_RunPodServiceServicer_to_server(RunPodService(), server)

        bind_address = f"0.0.0.0:50051"
        port = server.add_insecure_port(bind_address)

        if port == 0:
            print(f"❌ Failed to bind gRPC server to {bind_address}")
            return

        print(f"✅ gRPC server binding success at {bind_address} (port confirmed: {port})")
        await server.start()
        print(f"🚀 gRPC server has started and is ready to receive requests!")

        await server.wait_for_termination()

    except Exception as e:
        print(f"❗ Error while starting gRPC server: {e}")


async def handler(event):
    print("\n🚀 RunPod handler started")
    public_ip = os.environ.get('RUNPOD_PUBLIC_IP', 'localhost')
    tcp_port = int(os.environ.get('RUNPOD_TCP_PORT_50051', '50051'))
    print(f"🌐 Public IP: {public_ip}")
    print(f"🔌 TCP Port: {tcp_port}")

    print("🛠️  Starting gRPC server in background...")
    grpc_task = asyncio.create_task(start_grpc_server())

    print("⏳ Waiting for gRPC request on response queue...")
    result = await response_queue.get()
    print(f"✅ gRPC request received and processed: {result}")

    return {"result": result}


# RunPod 진입점: async 핸들러를 동기적으로 실행하기 위해 asyncio.run으로 감쌈
if __name__ == '__main__':
    runpod.serverless.start({'handler': handler})

