import os
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM

class LLMModel:
    def __init__(self, model_id):
        self.model_id = model_id
        self.tokenizer = None
        self.model = None
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.load_model()
    
    def load_model(self):
        # Hugging Face 토큰이 필요한 경우
        hf_token = os.environ.get("HF_TOKEN")
        
        self.tokenizer = AutoTokenizer.from_pretrained(
            self.model_id, 
            use_auth_token=hf_token
        )
        
        self.model = AutoModelForCausalLM.from_pretrained(
            self.model_id,
            device_map="auto",
            torch_dtype=torch.float16,
            use_auth_token=hf_token
        )
        
        print(f"모델 {self.model_id}가 {self.device}에 로드되었습니다.")
    
    def generate(self, prompt, max_new_tokens=100, temperature=0.7):
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.device)
        
        # 토큰 생성
        output_ids = self.model.generate(
            inputs.input_ids,
            max_new_tokens=max_new_tokens,
            temperature=temperature,
            do_sample=True,
            pad_token_id=self.tokenizer.eos_token_id
        )
        
        # 전체 출력 텍스트
        full_output = self.tokenizer.decode(output_ids[0], skip_special_tokens=True)
        
        # 프롬프트 이후의 생성 텍스트만 반환
        generated_text = full_output[len(prompt):]
        return generated_text
    
    def generate_stream(self, prompt, max_new_tokens=100, temperature=0.7):
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.device)
        
        # 스트리밍 생성을 위한 설정
        streamer = self.tokenizer.create_token_streamer()
        
        # 생성 시작
        self.model.generate(
            inputs.input_ids,
            max_new_tokens=max_new_tokens,
            temperature=temperature,
            do_sample=True,
            pad_token_id=self.tokenizer.eos_token_id,
            streamer=streamer
        )
        
        # 토큰 스트리밍
        for new_text in streamer:
            yield new_text
