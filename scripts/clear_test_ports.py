#!/usr/bin/env python3
import subprocess

REQUIRED_PORTS = {
    "tilt": 10352,
    "kafka": 9094,
    "redis": 6379,
    "mailpit_smtp": 10250,
    "mailpit_http": 8025,
    "llm": 11434
}

def kill_port(port):
    try:
        pids = subprocess.check_output(["lsof", "-t", "-i", f":{port}"], text=True).strip().split()
        for pid in pids:
            if pid:
                print(f"\033[33mKilling process {pid} occupying port {port}...\033[0m", flush=True)
                subprocess.run(["kill", "-9", pid], check=False)
    except subprocess.CalledProcessError:
        pass

def main():
    print("\033[36mCleaning up any existing processes on test ports...\033[0m", flush=True)
    for port in REQUIRED_PORTS.values():
        kill_port(port)

if __name__ == "__main__":
    main()
