from __future__ import annotations

from pathlib import Path


def read_topic_names(path: Path) -> list[str]:
    names: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped.startswith("name:") or stripped.startswith("- name:"):
            names.append(stripped.split(":", 1)[1].strip())
    return names


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    topics = read_topic_names(root / "infra/kafka/topics.yaml")
    readme = (root / "README.md").read_text(encoding="utf-8")
    missing = [topic for topic in topics if topic not in readme]
    if missing:
        print("README is missing topics:")
        for topic in missing:
            print(f"- {topic}")
        return 1
    print(f"validated {len(topics)} Kafka topics")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

