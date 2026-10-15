"""Server socket TCP multithread dengan Queue (3 worker thread, 5 koneksi)."""
import socket
import threading
import queue
import time

HOST = "127.0.0.1"
PORT = 5000
JUMLAH_WORKER = 3
JUMLAH_KONEKSI = 5


def log(pesan):
    # Satu kali write agar baris log dari banyak thread tidak bercampur
    print(pesan + "\n", end="", flush=True)


class Worker(threading.Thread):
    def __init__(self, nama, antrian):
        super().__init__(name=nama)
        self.antrian = antrian

    def run(self):
        while True:
            # get() memblokir (menunggu) selama queue kosong
            item = self.antrian.get()

            # Sentinel None = tanda berhenti untuk worker ini
            if item is None:
                log(f"[{self.name}] menerima sentinel, berhenti")
                self.antrian.task_done()
                break

            conn, addr = item
            try:
                log(f"[{self.name}] mengambil koneksi dari {addr}")
                time.sleep(1)  # simulasi proses agar pembagian kerja terlihat
                data = conn.recv(1024).decode("utf-8")
                log(f"[{self.name}] pesan dari {addr}: {data!r}")
                balasan = f"{data.upper()} (diproses oleh {self.name})"
                conn.sendall(balasan.encode("utf-8"))
            finally:
                conn.close()
                # Tandai item selesai diproses agar queue.join() bisa lanjut
                self.antrian.task_done()


def main():
    antrian = queue.Queue()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # SO_REUSEADDR: port tidak tertahan (TIME_WAIT) saat server di-restart
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()
    log(f"[Server] mendengarkan di {HOST}:{PORT}")

    workers = [Worker(f"Worker-{i}", antrian) for i in range(1, JUMLAH_WORKER + 1)]
    for w in workers:
        w.start()

    # Accept hanya menerima koneksi lalu memasukkannya ke queue (tanpa memproses pesan)
    for n in range(1, JUMLAH_KONEKSI + 1):
        conn, addr = server.accept()
        log(f"[Server] koneksi ke-{n} diterima dari {addr}, dimasukkan ke queue")
        antrian.put((conn, addr))

    # Satu sentinel (None) per worker agar semua worker keluar dari loop
    for _ in workers:
        antrian.put(None)

    # Tunggu semua item (koneksi + sentinel) selesai ditandai task_done()
    antrian.join()
    # Tunggu seluruh worker thread benar-benar selesai
    for w in workers:
        w.join()

    server.close()
    log("[Server] semua koneksi selesai, server berhenti")


if __name__ == "__main__":
    main()
