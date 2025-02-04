import os
import joblib
import pandas as pd
import xgboost as xgb

data_dir = "C:/Users/smhrd/scm_risk_detector_backend/raison/data"

# 📌 `joblib`을 사용하여 모델 불러오기
model_path = os.path.join(data_dir, "xgboost_stock_model.pkl")
loaded_model = joblib.load(model_path)

# 📌 새로운 데이터 입력 (셈플 : sentiment 0.7, OPEN 10000)
new_data = pd.DataFrame({
    'sentiment': [0.8],  # 감성 점수
    'OPEN': [100000]  # 시가 (OPEN)
})

# 📌 예측 수행
new_prediction = loaded_model.predict(new_data)

# 📌 예측된 변화율을 실제 주가로 변환
predicted_high = new_data['OPEN'][0] * (1 + new_prediction[0][0])
predicted_low = new_data['OPEN'][0] * (1 + new_prediction[0][1])
predicted_close = new_data['OPEN'][0] * (1 + new_prediction[0][2])

# 📌 상한가/하한가 30% 룰 적용
max_limit = new_data['OPEN'][0] * 1.3
min_limit = new_data['OPEN'][0] * 0.7

predicted_high = max(min(predicted_high, max_limit), min_limit)
predicted_low = max(min(predicted_low, max_limit), min_limit)
predicted_close = max(min(predicted_close, max_limit), min_limit)

# 📌 결과 출력
print(f"📈 예측 HIGH: {predicted_high:.2f}")
print(f"📉 예측 LOW: {predicted_low:.2f}")
print(f"📊 예측 CLOSE: {predicted_close:.2f}")
