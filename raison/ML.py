import joblib
import os
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split

# 📌 데이터 불러오기
data_dir = "C:/Users/smhrd/scm_risk_detector_backend/raison/data"
data_path = os.path.join(data_dir, "merged_processed_data.csv")
df = pd.read_csv(data_path)

# 📌 주가 변화율(%) 추가 (OPEN 대비 HIGH, LOW, CLOSE 변화율 계산)
df['high_change'] = (df['HIGH'] - df['OPEN']) / df['OPEN']
df['low_change'] = (df['LOW'] - df['OPEN']) / df['OPEN']
df['close_change'] = (df['CLOSE'] - df['OPEN']) / df['OPEN']

# 📌 입력(X)과 출력(Y) 설정
X = df[['sentiment', 'OPEN']]
Y = df[['high_change', 'low_change', 'close_change']]

# 📌 데이터 분할 (훈련 80%, 테스트 20%)
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

# 📌 XGBoost 모델 설정
xgb_model = xgb.XGBRegressor(
    objective='reg:squarederror',
    n_estimators=200,       # 트리 개수 증가
    learning_rate=0.03,     # 학습률 감소 (더 세밀하게 학습)
    max_depth=6,            # 트리 깊이 증가 (더 복잡한 패턴 학습)
    subsample=0.8,          # 샘플링 비율 (과적합 방지)
    colsample_bytree=0.8,   # 피처 샘플링 비율 (과적합 방지)
    min_child_weight=5,     # 최소 가중치 (불필요한 분할 방지)
    gamma=0.1,              # 리프 노드 추가 규제
    early_stopping_rounds=10
)

# 📌 모델 학습 (조기 종료 적용)
xgb_model.fit(X_train, Y_train, eval_set=[(X_test, Y_test)], verbose=True)

# 📌 `joblib`을 사용하여 모델 저장
model_path = os.path.join(data_dir, "xgboost_stock_model.pkl")
joblib.dump(xgb_model, model_path)

print(f"✅ XGBoost 모델 학습 완료 및 저장됨! (파일: {model_path})")