import time
import subprocess
from logger import get_logger

logger = get_logger(__name__)

def run_script():
    subprocess.run(["python", "common_questions_api.py"])

if __name__ == "__main__":
    while True:
        try:
            run_script()
            logger.info("Script executed. Waiting 5 minutes...")
            time.sleep(300)
        except:
            logger.info("Script not executed. Trying again in 10 seconds...")
            time.sleep(10)