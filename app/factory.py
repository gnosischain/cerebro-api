import yaml
import os
from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException, Request
from app.database import ClickHouseClient
from app.security import get_api_key, limiter, get_limit_string
from app.manifest import manifest
from app.config import settings

class DynamicRouter:
    def __init__(self):
        self.router = APIRouter()
        self.manual_config = self._load_manual_config()
        self._build_routes()

    def _load_manual_config(self):
        if os.path.exists(settings.API_CONFIG_PATH):
            with open(settings.API_CONFIG_PATH, "r") as f:
                return yaml.safe_load(f)
        return {}

    def _build_routes(self):
        # 1. Identify models to expose
        # Strategy: Expose all models starting with 'api_' AND any explicitly defined in yaml
        models_to_expose = set()
        
        # Auto-discovery
        for model_name in manifest.get_all_models():
            if model_name.startswith("api_"):
                models_to_expose.add(model_name)

        # Manual overrides
        manual_endpoints = self.manual_config.get("endpoints", [])
        manual_map = {ep["model"]: ep for ep in manual_endpoints}
        
        # 2. Generate Routes
        for model_name in models_to_expose:
            # Merge auto-detected settings with manual overrides if they exist
            manual_settings = manual_map.get(model_name, {})
            self._create_auto_route(model_name, manual_settings)

        # Also add any manual endpoints that might NOT start with api_ (e.g. fct_ tables)
        for ep in manual_endpoints:
            if ep["model"] not in models_to_expose:
                self._create_auto_route(ep["model"], ep)

    def _create_auto_route(self, model_name: str, override: dict):
        # --- Metadata Extraction ---
        dbt_node = manifest.get_model(model_name)
        columns = manifest.get_columns(model_name)
        dbt_tags = manifest.get_tags(model_name)

        # Clean up URL path: remove 'api_' prefix and replace underscores with slashes
        # e.g., api_consensus_validators_active_daily -> /consensus/validators/active/daily
        clean_name = model_name
        if clean_name.startswith("api_"):
            clean_name = clean_name[4:]
        
        url_path = override.get("path", f"/{clean_name.replace('_', '/')}")
        summary = override.get("summary", clean_name.replace("_", " ").title())
        
        # Grouping: Use dbt tags, but filter out common ones like 'production'
        ignored_tags = {'production', 'daily', 'hourly', 'view', 'table', 'incremental'}
        api_tags = [t for t in dbt_tags if t not in ignored_tags]
        if not api_tags: 
            api_tags = ["General"]
        
        if override.get("tags"):
            api_tags = override.get("tags")

        # --- Auto-Detect Parameters ---
        allowed_params = override.get("parameters", [])
        
        # If no manual params, detect them from columns
        if not allowed_params:
            # 1. Date Filters
            date_cols = [c for c in columns if 'Date' in columns[c] or 'Time' in columns[c] or c in ['date', 'timestamp', 'block_timestamp']]
            if date_cols:
                main_date_col = date_cols[0] # Pick first date col found
                allowed_params.append({"name": "start_date", "column": main_date_col, "operator": ">=", "type": "date"})
                allowed_params.append({"name": "end_date", "column": main_date_col, "operator": "<=", "type": "date"})
                
            # 2. Address Filters
            if 'address' in columns:
                allowed_params.append({"name": "address", "column": "address", "operator": "ILIKE", "type": "string"})
            
            # 3. Common IDs
            for col in ['project', 'sector', 'label', 'status']:
                if col in columns:
                    allowed_params.append({"name": col, "column": col, "operator": "=", "type": "string"})

        # Default ordering (Date DESC is usually best for timeseries)
        order_by = override.get("order_by")
        if not order_by:
            date_cols = [c for c in columns if 'Date' in columns[c] or 'Time' in columns[c] or c in ['date', 'timestamp']]
            if date_cols:
                order_by = f"{date_cols[0]} DESC"

        # --- Route Handler ---
        table_name = manifest.get_table_name(model_name)
        
        @limiter.limit(get_limit_string)
        async def dynamic_handler(
            request: Request,
            limit: int = Query(100, ge=1, le=5000),
            offset: int = Query(0, ge=0),
            _tier: str = Depends(get_api_key)
        ):
            sql = f"SELECT * FROM {table_name}"
            where_parts = []
            query_params = {"limit": limit, "offset": offset}

            # Process Filters
            for param in allowed_params:
                p_name = param["name"]
                p_col = param["column"]
                p_op = param.get("operator", "=")
                
                val = request.query_params.get(p_name)
                if val:
                    key = f"p_{p_name}"
                    # Handle LIKE/ILIKE for strings
                    if "LIKE" in p_op:
                        where_parts.append(f"{p_col} {p_op} %({key})s")
                        query_params[key] = f"%{val}%" if "%" not in val else val
                    else:
                        where_parts.append(f"{p_col} {p_op} %({key})s")
                        query_params[key] = val

            if where_parts:
                sql += " WHERE " + " AND ".join(where_parts)

            if order_by:
                sql += f" ORDER BY {order_by}"

            sql += " LIMIT %(limit)s OFFSET %(offset)s"

            try:
                data = ClickHouseClient.query(sql, query_params)
                return data
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        # --- Documentation Generation ---
        # Add column info to description
        col_doc = "\n".join([f"- **{k}**: {v}" for k,v in columns.items()])
        full_desc = f"{dbt_node.get('description', '')}\n\n**Columns:**\n{col_doc}"
        
        dynamic_handler.__doc__ = full_desc
        
        # Register
        self.router.add_api_route(
            path=url_path,
            endpoint=dynamic_handler,
            methods=["GET"],
            summary=summary,
            tags=api_tags,
            name=model_name
        )

router = DynamicRouter().router