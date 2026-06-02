from __future__ import annotations

import logging
import uvicorn


def main() -> int:
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )
    uvicorn.run("services.composer.api:app", host="0.0.0.0", port=8080)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
