import urllib.request
import time
import sys

url = 'http://127.0.0.1:5000/welcome'
last_err = None
for attempt in range(30):
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            body = resp.read().decode('utf-8', errors='replace')
            print('HTTP', resp.getcode())
            print('--- page snippet ---')
            print(body[:800])
            sys.exit(0)
    except Exception as e:
        last_err = e
        time.sleep(0.5)
print('Failed to reach', url, 'after retries:', last_err)
sys.exit(2)
