from __future__ import annotations

import uvicorn

def main() -> int:
    uvicorn.run("services.composer.api:app", host="0.0.0.0", port=8080)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
