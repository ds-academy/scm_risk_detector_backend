import os
from flask import Flask
from flask_cors import CORS
from module.route.chat import chat_bp
from module.route.session import session_bp

app = Flask(__name__)
CORS(app, supports_credentials=True)

# 환경 변수 로드
app.config["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")
app.config["GPT_MODEL_ID"] = os.getenv("GPT_MODEL_ID",  # Fine-Tuned model ID
                                       "ft:gpt-3.5-turbo-0125:personal:llm-experiment:9oAnm4r1")
app.config["MODEL_ID"] = os.getenv("MODEL_ID", "MLP-KTLim/llama-3-Korean-Bllossom-8B")
app.config["MODEL_PATH"] = "llama3/ft2_invest"
app.config["DEBUG"] = os.getenv("DEBUG", "False").lower() == "true"
app.config["PORT"] = int(os.getenv("PORT", 30000))


# 블루프린트 등록
app.register_blueprint(chat_bp)
app.register_blueprint(session_bp)

if __name__ == "__main__":
    if not app.config["OPENAI_API_KEY"]:
        raise ValueError("OPENAI_API_KEY 환경 변수가 설정되지 않았습니다.")

    app.run(host="0.0.0.0", port=app.config["PORT"], debug=app.config["DEBUG"])
