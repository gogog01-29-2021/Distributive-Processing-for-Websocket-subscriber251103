# mock_ingestor.py

import boto3
import json
import time
import sys
import uuid

# 사용법: python mock_ingestor.py [exchange] [product] [instance_id]
# 예시: python mock_ingestor.py binance abc A
if len(sys.argv) < 4:
    print("Usage: python mock_ingestor.py [exchange] [product] [instance_id (A or B)]")
    sys.exit(1)

EXCHANGE = sys.argv[1]
PRODUCT = sys.argv[2]
INSTANCE_ID = sys.argv[3] # 'A' or 'B'

# SQS 클라이언트 생성
sqs = boto3.client('sqs')

# 1. Validator가 구독할 SQS FIFO 큐 (인프라 담당자가 미리 생성)
# 예: raw.binance.abc.fifo
QUEUE_URL = f"https://sqs.ap-northeast-2.amazonaws.com/123456789/raw.{EXCHANGE}.{PRODUCT}.fifo"

print(f"Starting Mock Ingestor for: {EXCHANGE}-{PRODUCT}-{INSTANCE_ID}")
print(f"Targeting SQS FIFO Queue: {QUEUE_URL}\n")

seq_num = 1000 # 시퀀스 번호 시작점

try:
    while True:
        # 2. 가짜 데이터 생성
        data = {
            'seq': seq_num,
            'ts_recv_mock': time.time_ns(), # 가짜 수신 시각 (Validator가 비교할)
            'bids': [(f"200{seq_num % 100}", "1.5")],
            'asks': [(f"201{seq_num % 100}", "0.5")]
        }

        # 3. ★핵심★ SQS FIFO 메시지 전송
        sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(data),
            
            # Validator가 A/B를 구분하고 순서를 보장받는 핵심
            MessageGroupId=f"{EXCHANGE}-{PRODUCT}-{INSTANCE_ID}", 
            
            # Validator가 중복을 걸러낼 수 있도록 시퀀스 번호 사용
            MessageDeduplicationId=f"{INSTANCE_ID}-{seq_num}" 
        )

        print(f"[{INSTANCE_ID}] Sent seq = {seq_num}")

        # 4. 다음 시퀀스 번호 준비
        seq_num += 1
        
        # 5. 실시간성 조절 (A가 B보다 20ms 빠르게 시뮬레이션)
        if INSTANCE_ID == 'A':
            time.sleep(0.01) # A는 10ms 마다 전송
        else:
            time.sleep(0.03) # B는 30ms 마다 전송 (지연 시뮬레이션)

except KeyboardInterrupt:
    print("\nStopping mock ingestor...")