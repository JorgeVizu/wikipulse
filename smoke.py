"""Exercise ranking and checkpoint recovery with controlled events in local Kafka."""
import json
import subprocess
import time
from urllib.error import URLError
from urllib.request import urlopen


def get(port, path):
    with urlopen(f"http://127.0.0.1:{port}{path}", timeout=5) as response:
        return json.load(response)


def wait(read, label, timeout=100):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        try:
            result = read()
            if result:
                return result
        except (OSError, URLError):
            pass
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for {label}")


def publish(*events):
    base_ms = 1_800_000_000_000
    messages = []
    for edit_id, title, second in events:
        stamp = base_ms + second * 1000
        messages.append(json.dumps({
            "edit_id": edit_id, "title": title,
            "event_ms": stamp, "ingested_ms": stamp + 250,
        }))
    subprocess.run(
        ["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-console-producer.sh",
         "--bootstrap-server", "kafka:9092", "--topic", "wiki-edits"],
        input="\n".join(messages) + "\n", text=True, check=True,
    )


def ranking():
    return get(8090, "/api/ranking")


def main():
    job = wait(
        lambda: next((job["jid"] for job in get(8091, "/jobs/overview")["jobs"]
                      if job["name"] == "WikiPulse" and job["state"] == "RUNNING"), None),
        "running WikiPulse job",
    )

    def checkpoints(kind):
        return get(8091, f"/jobs/{job}/checkpoints")["latest"][kind]

    publish(("1", "Alfa", 0), ("1", "Alfa", 0), ("2", "Alfa", 1),
            ("3", "Beta", 2), ("4", "Gamma", 45))

    def initial_ranking():
        result = ranking()
        return result if len(result["rows"]) == 2 else None

    initial = wait(initial_ranking, "initial ranking")
    assert [(row["title"], row["edit_count"]) for row in initial["rows"]] == [
        ("Alfa", 2), ("Beta", 1),
    ], initial

    previous = wait(lambda: checkpoints("completed"), "first checkpoint")["id"]
    completed = wait(
        lambda: (snapshot if (snapshot := checkpoints("completed")) and snapshot["id"] > previous else None),
        "checkpoint after processing the first window",
    )
    subprocess.run(["docker", "compose", "restart", "taskmanager"], check=True)
    restored = wait(lambda: checkpoints("restored"), "restored checkpoint")
    assert restored["id"] >= completed["id"], restored

    publish(("5", "Gamma", 46), ("6", "Delta", 75))

    def recovered_ranking():
        result = ranking()
        if result["window_end"] and result["window_end"] > initial["window_end"] and len(result["rows"]) == 3:
            return result
        return None

    result = wait(recovered_ranking, "new window with recovered state")
    assert [(row["title"], row["edit_count"]) for row in result["rows"]] == [
        ("Alfa", 2), ("Gamma", 2), ("Beta", 1),
    ], result
    print(f"Restored checkpoint {restored['id']} and verified the ranking retained pre-restart edits.")


if __name__ == "__main__":
    main()
