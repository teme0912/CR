import urllib.request
import urllib.parse
import time
import sys

url = 'http://127.0.0.1:5000/register/individual'

unique_suffix = str(int(time.time()))[-6:]
phone = f'0701{unique_suffix}'
identity = f'TESTID{unique_suffix}'

data = {
    'client_type': 'Individual Client',
    'first_name': 'Test',
    'last_name': 'User',
    'gender': 'Male',
    'date_of_birth': '1990-01-01',
    'phone': phone,
    'email': f'testuser{unique_suffix}@example.com',
    'address': '123 Test Lane',
    'id_type': 'Passport',
    'id_number': identity,
    'occupation': 'Engineer',
    'source_of_funds': 'Salary',
    'risk_level': 'Low',
    'consent': 'yes',
}

payload = urllib.parse.urlencode(data).encode('utf-8')
headers = {'Content-Type': 'application/x-www-form-urlencoded'}

req = urllib.request.Request(url, data=payload, headers=headers)

# Wait for server to be ready
for attempt in range(20):
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            body = resp.read().decode('utf-8', errors='replace')
            print('HTTP', resp.getcode())
            print('--- Response snippet ---')
            print(body[:4000])
            sys.exit(0)
    except Exception as e:
        last_err = e
        time.sleep(0.5)

print('Request failed after retries:', last_err)
sys.exit(2)
