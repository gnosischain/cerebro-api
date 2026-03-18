"""Process entrypoint for cerebro-api.

Calls setup_logging() BEFORE importing the app so that all module-level
logging (config, manifest, factory) goes through the JSON formatter.
"""


def main():
    from app.observability import setup_logging

    setup_logging()

    import uvicorn

    from app.main import app  # noqa: E402 — intentionally late import

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8000,
        proxy_headers=True,
        log_config=None,
    )


if __name__ == "__main__":
    main()
