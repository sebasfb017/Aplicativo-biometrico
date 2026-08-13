import sys
from zk import ZK

def test_conn(ip):
    print(f"Testing TCP for {ip}...")
    zk = ZK(ip, port=4370, timeout=5, ommit_ping=True)
    try:
        conn = zk.connect()
        print(f"✅ Success TCP {ip}")
        conn.disconnect()
        return
    except Exception as e:
        print(f"❌ Failed TCP {ip}: {e}")
        
    print(f"Testing UDP for {ip}...")
    zk = ZK(ip, port=4370, timeout=5, ommit_ping=True, force_udp=True)
    try:
        conn = zk.connect()
        print(f"✅ Success UDP {ip}")
        conn.disconnect()
        return
    except Exception as e:
        print(f"❌ Failed UDP {ip}: {e}")

test_conn('192.168.52.6')
