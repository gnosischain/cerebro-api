import json
import os
import requests
from typing import Dict, Any, Optional, List
from app.config import settings

class ManifestLoader:
    _instance = None
    _models: Dict[str, Any] = {}

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ManifestLoader, cls).__new__(cls)
            cls._instance._load_manifest()
        return cls._instance

    def _load_manifest(self):
        data = None
        
        # 1. Try URL first
        if settings.DBT_MANIFEST_URL:
            try:
                print(f"🌐 Fetching manifest from {settings.DBT_MANIFEST_URL}...")
                response = requests.get(settings.DBT_MANIFEST_URL, timeout=30)
                if response.status_code == 200:
                    data = response.json()
                    print("✅ Manifest downloaded successfully.")
                else:
                    print(f"❌ Failed to download manifest: Status {response.status_code}")
            except Exception as e:
                print(f"❌ Error fetching manifest URL: {e}")

        # 2. Fallback to local file
        if not data and os.path.exists(settings.DBT_MANIFEST_PATH):
            try:
                print(f"📂 Loading manifest from local file: {settings.DBT_MANIFEST_PATH}")
                with open(settings.DBT_MANIFEST_PATH, 'r') as f:
                    data = json.load(f)
            except Exception as e:
                print(f"❌ Error loading local manifest: {e}")

        if not data:
            print("⚠️ No manifest loaded. API will define routes but metadata will be missing.")
            return

        # Index models
        for key, node in data.get("nodes", {}).items():
            if node.get("resource_type") == "model":
                name = node.get("name")
                self._models[name] = node
        
        print(f"✅ Loaded {len(self._models)} models from dbt manifest.")

    def get_all_models(self) -> List[str]:
        """Return a list of all model names."""
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
        """Returns a dict of column_name -> data_type"""
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

manifest = ManifestLoader()