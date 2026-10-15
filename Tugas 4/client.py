"""Client: menjalankan 5 thread client bersamaan, masing-masing mengirim pesan berbeda."""
import socket
import threading
import time

HOST = "127.0.0.1"
PORT = 5000
PERCOBAAN = 5       # jumlah retry jika server belum siap
JEDA_RETRY = 0.5    # detik

PESAN = [
    "halo dari client satu",
    "belajar pemrograman paralel",
    "socket dan queue itu menarik",
    "python thread worker",
    "tugas pertemuan empat",
]


def log(pesan):
    # Satu kali write agar baris log dari banyak thread tidak bercampur
    print(pesan + "\n", end="", flush=True)


def jalankan_client(nomor, pesan):
    nama = f"Client-{nomor}"
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Retry singkat jika server belum siap (ConnectionRefusedError)
        for percobaan in range(1, PERCOBAAN + 1):
            try:
                sock.connect((HOST, PORT))
                break
            except ConnectionRefusedError:
                if percobaan == PERCOBAAN:
                    log(f"[{nama}] GAGAL: server {HOST}:{PORT} menolak koneksi. "
                        "Jalankan server.py terlebih dahulu.")
                    return
                time.sleep(JEDA_RETRY)

        log(f"[{nama}] terhubung, mengirim: {pesan!r}")
        sock.sendall(pesan.encode("utf-8"))
        balasan = sock.recv(1024).decode("utf-8")
        log(f"[{nama}] balasan server: {balasan}")
    finally:
        # Socket ditutup setelah balasan diterima
        sock.close()


def main():
    threads = [
        threading.Thread(target=jalankan_client, args=(i, p), name=f"Client-{i}")
        for i, p in enumerate(PESAN, start=1)
    ]
    for t in threads:
        t.start()
    # Program utama menunggu semua client selesai
    for t in threads:
        t.join()
    log("[Main] semua client selesai")


if __name__ == "__main__":
    main()
