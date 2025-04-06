from websocket_server import WebsocketServer
from transformers import AutoModelForCausalLM, AutoTokenizer, TextIteratorStreamer
import torch
import threading
import os
import runpod


model_name = "Qwen/Qwen2.5-7B-Instruct"
model = AutoModelForCausalLM.from_pretrained(
    model_name,
    torch_dtype="auto",
    device_map="auto"
)
tokenizer = AutoTokenizer.from_pretrained(model_name)

def generate_streaming_response(prompt, client, server):
    messages = [
        {"role": "system", "content": "You are Qwen, created by Alibaba Cloud. You are a helpful assistant."},
        {"role": "user", "content": prompt}
    ]
    text = tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=True)
    inputs = tokenizer([text], return_tensors="pt").to(model.device)
    
    print(f"💬 Generating response for prompt: {prompt}")

    # 스트리밍 객체 생성
    streamer = TextIteratorStreamer(tokenizer, skip_prompt=True, skip_special_tokens=True)

    # 생성은 별도 스레드에서 비동기 실행
    generation_thread = threading.Thread(
        target=model.generate,
        kwargs={
            "inputs": inputs["input_ids"],
            "attention_mask": inputs["attention_mask"],
            "max_new_tokens": 512,
            "streamer": streamer
        }
    )
    generation_thread.start()

    # 토큰이 생성될 때마다 WebSocket으로 전송
    for token in streamer:
        server.send_message(client, token)

    server.send_message(client, "[✅ Done]")

def on_new_client(client, server):
    print(f"✅ New client connected: {client['id']}")

def on_message(client, server, message):
    print(f"💬 Received from client {client['id']}: {message}")
    if message.strip().lower() == "shutdown":
        print("⚠️ 'shutdown' command received, but ignored.")
        return


    # 생성 시작
    threading.Thread(target=generate_streaming_response, args=(message, client, server)).start()

def on_client_left(client, server):
    print(f"👋 Client {client['id']} disconnected")

def start_websocket():
    server = WebsocketServer(host="0.0.0.0", port=8765)
    server.set_fn_new_client(on_new_client)
    server.set_fn_client_left(on_client_left)
    server.set_fn_message_received(on_message)

    print("🚀 WebSocket server started on port 8765. Waiting for clients...")
    try:
        server.run_forever()
    except KeyboardInterrupt:
        print("🛑 Server manually stopped.")
    finally:
        print("🧹 Server cleanup complete.")

    return "WebSocket server stopped"

def handler(job):
    public_ip = os.environ.get('RUNPOD_PUBLIC_IP', 'localhost')
    tcp_port = int(os.environ.get('RUNPOD_TCP_PORT_8765', '8765'))

    print(f"🌐 Public IP: {public_ip}")
    print(f"🔌 TCP Port: {tcp_port}")

    runpod.serverless.progress_update(job, f"Public IP: {public_ip}, TCP Port: {tcp_port}")

    result = start_websocket()

    return {
        "message": result,
        "public_ip": public_ip,
        "tcp_port": tcp_port
    }

if __name__ == '__main__':
    runpod.serverless.start({
        'handler': handler,
        "return_aggregate_stream": True,
    })


