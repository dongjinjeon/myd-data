from config import CONF
import sqlite3
from datetime import datetime, timedelta

class CheckStatus:
    def get_current_status(self, job_id):
        conn = sqlite3.connect(CONF['CSV_STATUS_DB'])
        c = conn.cursor()
        c.execute("""select status, fail_log, cast(strftime('%s', create_datetime) as int) as job_start_time from job_status 
                        where job_id = ?""", [job_id])
        res = c.fetchone()
        c.close()
        conn.close()
        return res

    def clear_old_data(self):
        conn = sqlite3.connect(CONF['CSV_STATUS_DB'])
        c = conn.cursor()

        # 일주일 이전의 datetime 계산
        one_week_ago = datetime.now() - timedelta(days=7)

        # 일주일 이전인 row를 제거하는 쿼리 실행
        delete_query = "DELETE FROM job_status WHERE create_datetime < ?"
        c.execute(delete_query, (one_week_ago,))
        conn.commit()

        delete_query = "DELETE FROM csv_rows WHERE update_datetime < ?"
        c.execute(delete_query, (one_week_ago,))
        conn.commit()
        conn.close()
