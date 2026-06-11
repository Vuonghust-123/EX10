import socket
import threading
import json
import time
import sys
import math

if len(sys.argv) < 2:
    print("❌ Cách dùng: python3 worker.py <worker_id>")
    sys.exit(1)

WORKER_ID = int(sys.argv[1])

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 8000


# ==========================================
# PRIME COUNTING
# ==========================================

def count_primes(n):

    prime_count = 0

    for num in range(2, n + 1):

        is_prime = True

        limit = int(math.sqrt(num))

        for i in range(2, limit + 1):

            if num % i == 0:
                is_prime = False
                break

        if is_prime:
            prime_count += 1

    return prime_count


# ==========================================
# HEARTBEAT
# ==========================================

def send_heartbeat(sock):

    while True:

        try:

            hb = {
                "type": "HEARTBEAT",
                "worker_id": WORKER_ID
            }

            sock.sendall(
                (
                    json.dumps(hb)
                    + "\n"
                ).encode("utf-8")
            )

            time.sleep(2)

        except Exception:

            print(
                "🔴 Không thể gửi heartbeat."
            )

            break


# ==========================================
# MAIN
# ==========================================

def main():

    try:

        sock = socket.socket(
            socket.AF_INET,
            socket.SOCK_STREAM
        )

        sock.connect(
            (MASTER_HOST, MASTER_PORT)
        )

        print(
            f"🔄 Worker {WORKER_ID} "
            f"đang kết nối tới Master..."
        )

    except Exception as e:

        print(
            f"❌ Không kết nối được Master: {e}"
        )

        return

    register_msg = {
        "type": "REGISTER",
        "worker_id": WORKER_ID
    }

    sock.sendall(
        (
            json.dumps(register_msg)
            + "\n"
        ).encode("utf-8")
    )

    threading.Thread(
        target=send_heartbeat,
        args=(sock,),
        daemon=True
    ).start()

    buffer = ""

    while True:

        try:

            data = sock.recv(4096)

            if not data:

                print(
                    "🔴 Master đã đóng kết nối."
                )

                break

            buffer += data.decode("utf-8")

            while "\n" in buffer:

                line, buffer = buffer.split(
                    "\n",
                    1
                )

                if not line.strip():
                    continue

                msg = json.loads(line)

                if msg["type"] == "TASK":

                    task_id = msg["task_id"]
                    n = msg["input"]

                    print(
                        f"\n📥 [TASK] "
                        f"Nhận Task {task_id}"
                    )

                    print(
                        f"🔢 Đếm số nguyên tố <= {n}"
                    )

                    start = time.time()

                    result = count_primes(n)

                    elapsed = (
                        time.time() - start
                    )

                    print(
                        f"⏳ Task {task_id} "
                        f"xử lý xong sau "
                        f"{elapsed:.2f}s"
                    )

                    print(
                        "⌛ Giả lập xử lý thêm 10 giây..."
                    )

                    # để dễ test kill worker
                    time.sleep(10)

                    result_msg = {
                        "type": "RESULT",
                        "worker_id": WORKER_ID,
                        "task_id": task_id,
                        "output": result
                    }

                    sock.sendall(
                        (
                            json.dumps(result_msg)
                            + "\n"
                        ).encode("utf-8")
                    )

                    print(
                        f"✅ Đã gửi kết quả "
                        f"Task {task_id}"
                    )

        except Exception as e:

            print(
                f"❌ Lỗi Worker "
                f"{WORKER_ID}: {e}"
            )

            break

    sock.close()


if __name__ == "__main__":
    main()
