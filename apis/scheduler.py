import time
import subprocess

def run_script():
    subprocess.run(["python", "common_questions_api.py"])

if __name__ == "__main__":
    while True:
        run_script()
        print("Script executed. Waiting 5 minutes...")
        time.sleep(300)