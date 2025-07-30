# -*- coding: utf-8 -*-
"""
Created on Mon Jul 28 14:30:22 2025
@author: hks9999
"""

import re
import pymysql
from datetime import datetime
import sys
import os

root_dir = r"c:\\apache_log"

# DB 연결 정보 + 테이블명
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root',
    'database': 'weblog',
    'charset': 'utf8mb4',
    'table': 'webserver'
}

# 아파치 로그 정규식
LOG_PATTERN = re.compile(
    r'(?P<ip>[\d\.]+)\s+-\s+-\s+\[(?P<datetime>[^\]]+)\]\s+'
    r'"(?P<request>[^"]+)"\s+(?P<status>\d+)\s+(?P<size>\d+|-)\s+'
    r'"(?P<referrer>[^"]*)"\s+"(?P<user_agent>[^"]*)"'
)

def parse_log_line(line):
    match = LOG_PATTERN.match(line)
    if not match:
        raise ValueError(f"로그 파싱 실패: {line}")

    log = match.groupdict()

    try:
        log_time = datetime.strptime(log['datetime'], "%d/%b/%Y:%H:%M:%S %z")
        log_time = log_time.astimezone().replace(tzinfo=None)
    except Exception as e:
        raise ValueError(f"시간 파싱 오류: {e}")

    return {
        'ip': log['ip'],
        'log_time': log_time,
        'request': log['request'],
        'response_code': int(log['status']),
        'response_size': int(log['size']) if log['size'].isdigit() else 0,
        'referrer': log['referrer'],
        'user_agent': log['user_agent']
    }

def main():
    try:
        conn = pymysql.connect(**{k: v for k, v in DB_CONFIG.items() if k != 'table'})
        cursor = conn.cursor()
        table = DB_CONFIG['table']

        # 테이블 새로 생성
        cursor.execute(f"DROP TABLE IF EXISTS `{table}`")
        cursor.execute(f"""
            CREATE TABLE `{table}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                ip VARCHAR(45),
                log_time DATETIME,
                request TEXT,
                response_code INT,
                response_size BIGINT,
                referrer TEXT,
                user_agent TEXT
            )
        """)
        conn.commit()

        insert_sql = f"""
            INSERT INTO `{table}` 
            (ip, log_time, request, response_code, response_size, referrer, user_agent)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        batch_count = 0
        batch_size = 100000

        for dirpath, _, filenames in os.walk(root_dir):
            for filename in filenames:
                filepath = os.path.join(dirpath, filename)
                print(f"[처리중] {filepath}")
                try:
                    with open(filepath, "r", encoding='utf-8', errors='ignore') as f:
                        for line in f:
                            try:
                                log = parse_log_line(line.strip())

                                values = (
                                    log['ip'],
                                    log['log_time'],
                                    log['request'],
                                    log['response_code'],
                                    log['response_size'],
                                    log['referrer'],
                                    log['user_agent']
                                )

                                cursor.execute(insert_sql, values)
                                batch_count += 1

                                if batch_count >= batch_size:
                                    conn.commit()
                                    print(f"[커밋됨] {batch_count}건 삽입 완료")
                                    batch_count = 0

                            except ValueError as ve:
                                print(f"[무시된 라인] {filepath}: {ve}")
                except Exception as e:
                    print(f"[파일 오류] {filepath} 처리 중 오류: {e}")
                    conn.rollback()
                    sys.exit(1)

        # 남은 데이터 커밋
        if batch_count > 0:
            conn.commit()
            print(f"[최종 커밋] {batch_count}건 삽입 완료")

        print("모든 로그 파일을 성공적으로 처리했습니다.")

    except pymysql.MySQLError as err:
        print(f"[DB 오류] {err}")
        sys.exit(1)

    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    main()

