import socket
import threading
import json
import time

MASTER_HOST = "127.0.0.1"
MASTER_PORT = 8000

worker_table = {}
task_table = {}
task_queue = []

lock = threading.Lock()


# ==========================================
# RECOVERY
# ==========================================

def recover_worker_tasks(worker_id):

    for task in task_table.values():

        if (
            task["assigned_worker"] == worker_id
            and task["status"] == "RUNNING"
        ):

            task["status"] = "READY"
            task["assigned_worker"] = -1

            task_queue.append(task)

            print(
                f"🔄 [RECOVERY] "
                f"Task {task['task_id']} "
                f"được đưa trở lại hàng đợi"
            )


# ==========================================
# HANDLE WORKER
# ==========================================

def handle_worker(conn, addr):

    worker_id = None

    try:

        buffer = ""

        while True:

            data = conn.recv(4096)

            if not data:
                break

            buffer += data.decode("utf-8")

            while "\n" in buffer:

                line, buffer = buffer.split("\n", 1)

                if not line.strip():
                    continue

                msg = json.loads(line)

                msg_type = msg["type"]

                with lock:

                    # -------------------------
                    # REGISTER
                    # -------------------------
                    if msg_type == "REGISTER":

                        worker_id = msg["worker_id"]

                        worker_table[worker_id] = {
                            "conn": conn,
                            "alive": True,
                            "load": 0,
                            "last_heartbeat": time.time()
                        }

                        print(
                            f"🟢 [REGISTER] "
                            f"Worker {worker_id} "
                            f"đã kết nối từ {addr}"
                        )

                    # -------------------------
                    # HEARTBEAT
                    # -------------------------
                    elif msg_type == "HEARTBEAT":

                        wid = msg["worker_id"]

                        if wid in worker_table:

                            worker_table[wid][
                                "last_heartbeat"
                            ] = time.time()

                    # -------------------------
                    # RESULT
                    # -------------------------
                    elif msg_type == "RESULT":

                        task_id = msg["task_id"]

                        print(
                            f"✅ [RESULT] "
                            f"Task {task_id} "
                            f"hoàn thành bởi Worker "
                            f"{worker_id}"
                        )

                        if task_id in task_table:

                            task_table[task_id][
                                "status"
                            ] = "COMPLETED"

                        if worker_id in worker_table:

                            worker_table[worker_id][
                                "load"
                            ] = max(
                                0,
                                worker_table[worker_id]["load"] - 1
                            )

    except Exception as e:

        print(
            f"❌ Lỗi Worker "
            f"{worker_id}: {e}"
        )

    finally:

        with lock:

            if (
                worker_id is not None
                and worker_id in worker_table
            ):

                worker_table[worker_id][
                    "alive"
                ] = False

                print(
                    f"🔴 Worker "
                    f"{worker_id} "
                    f"đã ngắt kết nối"
                )

                recover_worker_tasks(worker_id)

        conn.close()


# ==========================================
# HEARTBEAT MONITOR
# ==========================================

def monitor_heartbeat():

    while True:

        time.sleep(2)

        now = time.time()

        with lock:

            for worker_id, info in list(worker_table.items()):

                if (
                    info["alive"]
                    and now - info["last_heartbeat"] > 6
                ):

                    info["alive"] = False

                    print(
                        f"⚠️ [TIMEOUT] "
                        f"Worker {worker_id} "
                        f"không gửi heartbeat"
                    )

                    recover_worker_tasks(worker_id)


# ==========================================
# LEAST LOADED SCHEDULER
# ==========================================

def scheduler():

    while True:

        time.sleep(0.5)

        selected_worker = None
        selected_conn = None
        task = None

        with lock:

            if not task_queue:
                continue

            min_load = float("inf")

            for wid, info in worker_table.items():

                if (
                    info["alive"]
                    and info["load"] < min_load
                ):

                    min_load = info["load"]

                    selected_worker = wid
                    selected_conn = info["conn"]

            if selected_worker is None:
                continue

            task = task_queue.pop(0)

            task["status"] = "RUNNING"
            task["assigned_worker"] = selected_worker

            worker_table[selected_worker][
                "load"
            ] += 1

        try:

            task_msg = {
                "type": "TASK",
                "task_id": task["task_id"],
                "operation": task["operation"],
                "input": task["input"]
            }

            selected_conn.sendall(
                (
                    json.dumps(task_msg)
                    + "\n"
                ).encode("utf-8")
            )

            print(
                f"🚀 [SCHEDULE] "
                f"Task {task['task_id']} "
                f"-> Worker {selected_worker}"
            )

        except Exception as e:

            print(
                f"❌ Gửi task thất bại "
                f"cho Worker "
                f"{selected_worker}: {e}"
            )

            with lock:

                task["status"] = "READY"
                task["assigned_worker"] = -1

                task_queue.append(task)

                if selected_worker in worker_table:

                    worker_table[selected_worker][
                        "alive"
                    ] = False

                    worker_table[selected_worker][
                        "load"
                    ] = max(
                        0,
                        worker_table[selected_worker]["load"] - 1
                    )

                recover_worker_tasks(selected_worker)


# ==========================================
# MAIN
# ==========================================

def main():
while len(worker_table) < 2:
    time.sleep(1)

print("✅ Đã có đủ worker, bắt đầu tạo task")
    # tạo task mẫu

    for i in range(1,5):

        task = {
            "task_id": i,
            "operation": "prime_count",
            "input": 50000,
            "status": "READY",
            "assigned_worker": -1
        }

        task_queue.append(task)
        task_table[i] = task

    server = socket.socket(
        socket.AF_INET,
        socket.SOCK_STREAM
    )

    server.setsockopt(
        socket.SOL_SOCKET,
        socket.SO_REUSEADDR,
        1
    )

    server.bind(
        (MASTER_HOST, MASTER_PORT)
    )

    server.listen()

    print(
        f"🖥️ [MASTER] "
        f"Đang chạy tại "
        f"{MASTER_HOST}:{MASTER_PORT}"
    )

    threading.Thread(
        target=monitor_heartbeat,
        daemon=True
    ).start()

    threading.Thread(
        target=scheduler,
        daemon=True
    ).start()

    while True:

        conn, addr = server.accept()

        threading.Thread(
            target=handle_worker,
            args=(conn, addr),
            daemon=True
        ).start()


if __name__ == "__main__":
    main()
