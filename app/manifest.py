import hashlib
import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import requests

from app.config import settings
from app.observability import cerebro_api_manifest_models_loaded, log_event

logger = logging.getLogger("cerebro_api.manifest")


class ManifestLoader:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ManifestLoader, cls).__new__(cls)
            cls._instance._models = {}
            cls._instance._etag = None
            cls._instance._last_modified = None
            cls._instance._hash = None
            cls._instance._last_error = None
            cls._instance._load_manifest(allow_fallback=True, conditional=False)
        return cls._instance

    def _hash_bytes(self, data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    def _load_manifest(self, allow_fallback: bool, conditional: bool) -> bool:
        data = None
        raw_bytes = None
        errors = []
        new_etag = None
        new_last_modified = None
        source = None
        self._last_error = None

        # 1. Try URL first
        if settings.DBT_MANIFEST_URL:
            try:
                log_event(logger, "manifest_refresh", action="fetch_url")
                headers = {}
                if conditional:
                    if self._etag:
                        headers["If-None-Match"] = self._etag
                    if self._last_modified:
                        headers["If-Modified-Since"] = self._last_modified
                response = requests.get(settings.DBT_MANIFEST_URL, timeout=30, headers=headers)
                if response.status_code == 304:
                    log_event(logger, "manifest_refresh", action="not_modified_304")
                    return False
                if response.status_code == 200:
                    raw_bytes = response.content
                    try:
                        data = response.json()
                        source = "url"
                        new_etag = response.headers.get("ETag")
                        new_last_modified = response.headers.get("Last-Modified")
                        log_event(logger, "manifest_refresh", action="downloaded")
                    except Exception as e:
                        msg = f"Error parsing manifest JSON from URL: {e}"
                        errors.append(msg)
                        log_event(logger, "manifest_refresh", level=logging.ERROR, action="parse_error", error=msg)
                else:
                    msg = f"Failed to download manifest: status {response.status_code}"
                    errors.append(msg)
                    log_event(logger, "manifest_refresh", level=logging.ERROR, action="http_error", status_code=response.status_code)
            except Exception as e:
                msg = f"Error fetching manifest URL: {e}"
                errors.append(msg)
                log_event(logger, "manifest_refresh", level=logging.ERROR, action="fetch_error", error=str(e))

        # 2. Fallback to local file
        if not data and allow_fallback and os.path.exists(settings.DBT_MANIFEST_PATH):
            try:
                log_event(logger, "manifest_refresh", action="load_file", path=settings.DBT_MANIFEST_PATH)
                with open(settings.DBT_MANIFEST_PATH, "rb") as f:
                    raw_bytes = f.read()
                data = json.loads(raw_bytes.decode("utf-8"))
                source = "file"
            except Exception as e:
                msg = f"Error loading local manifest: {e}"
                errors.append(msg)
                log_event(logger, "manifest_refresh", level=logging.ERROR, action="file_error", error=str(e))

        if not data:
            log_event(logger, "manifest_refresh", level=logging.WARNING, action="no_manifest_loaded")
            if errors:
                self._last_error = " | ".join(errors)
            else:
                self._last_error = "No manifest loaded."
            return False

        new_hash = None
        if raw_bytes is not None:
            new_hash = self._hash_bytes(raw_bytes)
        else:
            new_hash = self._hash_bytes(json.dumps(data, sort_keys=True).encode("utf-8"))

        if self._hash and new_hash == self._hash:
            if source == "file":
                self._etag = None
                self._last_modified = None
            if source == "url":
                if new_etag:
                    self._etag = new_etag
                if new_last_modified:
                    self._last_modified = new_last_modified
            log_event(logger, "manifest_refresh", action="unchanged_hash")
            return False

        # Index models
        new_models: Dict[str, Any] = {}
        for key, node in data.get("nodes", {}).items():
            if node.get("resource_type") == "model":
                name = node.get("name")
                new_models[name] = node

        self._models = new_models
        self._hash = new_hash

        if source == "file":
            self._etag = None
            self._last_modified = None
        if new_etag:
            self._etag = new_etag
        if new_last_modified:
            self._last_modified = new_last_modified

        self._last_error = None

        log_event(logger, "manifest_refresh", action="loaded", model_count=len(self._models), source=source)
        cerebro_api_manifest_models_loaded.set(len(self._models))
        return True

    def reload_if_changed(self) -> Tuple[bool, Optional[str]]:
        changed = self._load_manifest(allow_fallback=False, conditional=True)
        if changed:
            return True, None
        if self._last_error:
            return False, self._last_error
        return False, None

    def get_all_models(self) -> List[str]:
        return list(self._models.keys())

    def get_model(self, model_name: str) -> Optional[Dict[str, Any]]:
        return self._models.get(model_name)

    def get_table_name(self, model_name: str) -> str:
        node = self.get_model(model_name)
        if node:
            schema = node.get("schema", "default")
            alias = node.get("alias", model_name)
            return f"{schema}.{alias}"
        return model_name

    def get_columns(self, model_name: str) -> Dict[str, str]:
        node = self.get_model(model_name)
        if not node:
            return {}

        cols = {}
        for col_name, col_meta in node.get("columns", {}).items():
            cols[col_name] = col_meta.get("data_type", "String")
        return cols

    def get_tags(self, model_name: str) -> List[str]:
        node = self.get_model(model_name)
        if not node:
            return []
        return node.get("tags", [])

    def model_count(self) -> int:
        return len(self._models)


manifest = ManifestLoader()
