import boto3
import json
import time
import threading
import traceback
# 1. SQS FIFO 큐에서 데이터를 수신하고 처리하는 Validator 클래스 2.Snapshot processing 3.Mainlogic for execution: multi threading
class Validator(threading.Thread):
    def __init__(self, exchange, product):
        super().__init__()
        self.name = f"Validator-{exchange}-{product}"
        
        self.exchange = exchange
        self.product = product
        
        self.sqs = boto3.client('sqs', region_name='ap-northeast-2')
        # 큐 url 변경 필요
        self.queue_url = "https://sqs.ap-northeast-2.amazonaws.com/337909762204/queue.fifo"
        
        # 핵심 상태 변수: 다음 발행할 시퀀스 번호
        # (실제로는 스냅샷 동기화로 이 값을 초기화해야 함)
        self.next_seq_to_publish = 101 # 테스트를 위해 101로 가정
        
        # 5ms Peer Wait 및 재정렬을 위한 내부 버퍼
        self.buffer = {}
        
        # 20ms 누락 감지 타이머
        self.missing_seq_timer = None

    def run(self):
        print(f"[{self.name}] 시작. seq {self.next_seq_to_publish}부터 처리 시작.")
        while True:
            # SQS에서 배치(예시 10개)만큼 데이터
            messages = self._fetch_messages_from_sqs(batch_size=10, wait_time=1)

            # 수신한 배치를 내부 버퍼에 추가
            self._process_batch_into_buffer(messages)

            # 버퍼를 처리하며 '즉시' 발행 시도
            self._process_buffer_and_publish()

            # 20ms 누락(Missing) 타임아웃 확인
            self._check_for_missing_data_timeout()
    
    # list 반환
    def _fetch_messages_from_sqs(self, batch_size, wait_time):
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=batch_size,
                WaitTimeSeconds=wait_time, # Long Polling
                AttributeNames=['All'] # GroupId 등을 가져오기 위해
            )
            return response.get('Messages', [])
        except Exception as e:
            print(f"[{self.name}] SQS 수신 오류: {e}")
            traceback.print_exc()
            return []

    def _process_batch_into_buffer(self, messages):
        for msg in messages:
            try:
                group_id = msg['Attributes']['MessageGroupId'] # 메세지 그룹 ID 여기서는 binance-abc-A라 가정
                instance_id = group_id.split('-')[-1] # 'A' 또는 'B'
                
                body = json.loads(msg['Body'])
                seq = body['seq'] # 가져온 메세지의 시퀀스 넘버

                # 이미 처리한(발행한) 시퀀스 번호는 폐기
                if seq < self.next_seq_to_publish:
                    # SQS에서 메시지 삭제 (중요)
                    self._delete_message(msg['ReceiptHandle'])
                    continue

                # 버퍼에 해당 시퀀스 항목이 없으면 새로 생성
                if seq not in self.buffer:
                    self.buffer[seq] = {
                        'first_seen_local': time.time() # Peer Wait를 위한 로컬 시간 기록
                    }
                
                # 버퍼에 A 또는 B 데이터 저장
                if instance_id not in self.buffer[seq]:
                    self.buffer[seq][instance_id] = body
                
                # SQS에서 메시지 삭제 (일단 버퍼에 저장했으므로)
                self._delete_message(msg['ReceiptHandle'])

            except Exception as e:
                print(f"[{self.name}] 메시지 처리 오류: {e}")
                traceback.print_exc()

    def _process_buffer_and_publish(self):
        while True:
            seq_to_check = self.next_seq_to_publish
            
            if seq_to_check not in self.buffer:
                # 버퍼에 없으면
                if not self.missing_seq_timer:
                    # 타이머가 없다면 타이머 설정
                    self.missing_seq_timer = (seq_to_check, time.time())
                # 발행 중단하고 다음 SQS 배치를 기다림
                break

            entry = self.buffer[seq_to_check]
            a_data = entry.get('A')
            b_data = entry.get('B')

            winner = None

            # Peer Wait (A, B 둘 다 도착했는가?)
            if a_data and b_data:
                winner = a_data if a_data['ts_recv_mock'] <= b_data['ts_recv_mock'] else b_data    
            # Peer Wait (A, B 중 하나만 도착했는가?)
            elif a_data or b_data:
                elapsed = time.time() - entry['first_seen_local']
                # 5ms가 지났는가? (Peer Wait Timeout)
                if elapsed > 0.005:
                    # 5ms 지나도 Peer가 안 오면, 먼저 온 놈을 승자로 처리
                    winner = a_data if a_data else b_data
                else:
                    # 5ms가 안 지났으면: Peer를 더 기다림. 발행 중단.
                    break
            
            # 발행(Publish)
            if winner:
                # (실제로는 여기서 SQS/SNS로 발행해야 함)
                print(f"✓ [{self.name}] 발행 성공! [Seq: {seq_to_check}] [Winner: {winner.get('instance_id', 'N/A')}]")
                
                self.next_seq_to_publish += 1
                self.missing_seq_timer = None
                
                # 처리 완료된 버퍼 항목 삭제
                del self.buffer[seq_to_check]

    def _check_for_missing_data_timeout(self):
        if not self.missing_seq_timer:
            return

        seq_waiting_for, start_time = self.missing_seq_timer
        
        if seq_waiting_for == self.next_seq_to_publish:
            elapsed = time.time() - start_time
            
            if elapsed > 0.020:
                print(f"✗ [{self.name}] 데이터 누락 감지! [Seq: {seq_waiting_for}]")
                self._request_new_snapshot()

    def _request_new_snapshot(self):
        print(f"⚠ [{self.name}] 버퍼 비우고 스냅샷 재동기화 요청...")
        self.buffer.clear()
        self.missing_seq_timer = None
        
        # (실제로는 여기서 API로 새 스냅샷을 받고, next_seq_to_publish를
        #  새 스냅샷의 시퀀스 번호로 리셋해야 함)
        # 예: self.next_seq_to_publish = get_new_snapshot_seq()
        
        # 여기서는 임시로 5초 뒤 10개 건너뛰고 재시작한다고 가정
        time.sleep(5)
        self.next_seq_to_publish += 10 # 103 -> 113으로 강제 점프
        print(f"[{self.name}] 스냅샷 복구 완료. [Seq: {self.next_seq_to_publish}]부터 재시작.")

    def _delete_message(self, receipt_handle):
        try:
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
        except Exception as e:
            print(f"[{self.name}] 메시지 삭제 오류: {e}")
            traceback.print_exc()

# --- 테스트를 위한 메인 실행 ---
if __name__ == "__main__":
    # (실제로는 인프라 담당자가 9개의 Validator 스레드를 관리)
    
    # 이 Validator는 'binance'의 'abc' 종목만 담당함
    validator_thread = Validator(exchange="binance", product="abc")
    validator_thread.start()
    
    # (다른 Validator 스레드들도 여기서 시작)
    # validator_coinbase_abc = Validator(exchange="coinbase", product="abc")
    # validator_coinbase_abc.start()
