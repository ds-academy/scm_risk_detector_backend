import numpy as np
import pandas as pd
import ruptures as rpt


def calculate_risk_scores(df: pd.DataFrame, symbol: str, n_bkps=5, smoothing_alpha=0.3):
    """
    df에는 최소한 ["date", "close", "volume"] 컬럼이 존재해야 함.
    - close, volume 을 이용해 일별 risk value 계산
    - 수치적 안정성:
      1) 변동률 계산 후 MinMax(0~1) 스케일링
      2) 스무딩(EMA) 후, 매우 작은 값(1e-5 미만)은 0으로 clip
      3) 0~100%로 확장
    - 최종 리턴: (symbol, date, risk_value) 형태의 DataFrame
    """

    # 1. 입력 데이터 검증
    if "close" not in df.columns or "volume" not in df.columns:
        raise ValueError("Input DataFrame must contain 'close' and 'volume' columns.")

    price_list = df["close"].values
    volume_list = df["volume"].values

    if len(price_list) != len(volume_list):
        raise ValueError("Price list and Volume list must have the same length.")
    if len(price_list) < 2:
        # 데이터가 2개 미만이면 수익률/거래량 변동률 계산 불가
        return pd.DataFrame(columns=["symbol", "date", "risk_value"])

    # 2. 변동률 계산
    returns = (price_list[1:] - price_list[:-1]) / (price_list[:-1] + 1e-9)
    vol_changes = (volume_list[1:] - volume_list[:-1]) / (volume_list[:-1] + 1e-9)

    # 3. (returns, vol_changes) 2차원 특징 벡터
    features = np.column_stack((returns, vol_changes))

    # 4. ruptures Binseg 알고리즘 사용 (RBF 커널)
    model = rpt.Binseg(model="rbf").fit(features)
    bkps = model.predict(n_bkps=n_bkps)  # n_bkps 개의 세그먼트로 분할

    # 5. 각 세그먼트별 위험도 계산
    risk_raw = np.zeros(len(returns))
    start_idx = 0
    for end_idx in bkps:
        segment_data = features[start_idx:end_idx]
        if len(segment_data) == 0:
            continue

        std_return = np.std(segment_data[:, 0])
        std_volume = np.std(segment_data[:, 1])
        segment_volatility = np.sqrt(std_return ** 2 + std_volume ** 2)

        risk_raw[start_idx:end_idx] = segment_volatility
        start_idx = end_idx

    # 6. MinMax Scaling (0~1)
    min_val, max_val = risk_raw.min(), risk_raw.max()
    if (max_val - min_val) < 1e-9:
        risk_scaled = np.full_like(risk_raw, 0.5)
    else:
        risk_scaled = (risk_raw - min_val) / (max_val - min_val)

    # 7. 스무딩(EMA)
    smoothing_alpha = 0.3
    risk_smoothed = np.zeros_like(risk_scaled)
    risk_smoothed[0] = risk_scaled[0]
    for i in range(1, len(risk_scaled)):
        risk_smoothed[i] = (
                smoothing_alpha * risk_scaled[i]
                + (1 - smoothing_alpha) * risk_smoothed[i - 1]
        )

    # 8. 아주 작은 값(1e-5 미만)은 0으로 clip
    risk_clipped = np.where(risk_smoothed < 1e-5, 0, risk_smoothed)

    # 9. 최종 0~100으로 확장
    risk_percent = risk_clipped * 100.0

    # 결과 DataFrame (df에서 첫 행은 변동률 계산 불가이므로 제외)
    result_df = df.iloc[1:].copy()
    result_df["symbol"] = symbol
    result_df["risk_value"] = risk_percent

    return result_df[["symbol", "date", "risk_value"]]
