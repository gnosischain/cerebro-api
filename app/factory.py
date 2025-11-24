import yaml
import os
import re
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, Query, HTTPException, Request
from app.database import ClickHouseClient
from app.security import get_api_key, check_tier_access
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
        """
        Build routes for models that:
        1. Start with 'api_' prefix
        2. Have the 'production' tag
        
        Uses remaining tags for grouping in Swagger UI.
        """
        models_to_expose = set()

        # Auto-discovery: Only expose models with 'api_' prefix AND 'production' tag
        for model_name in manifest.get_all_models():
            if model_name.startswith("api_"):
                dbt_tags = manifest.get_tags(model_name)
                if "production" in dbt_tags:
                    models_to_expose.add(model_name)

        # Manual overrides from config
        manual_endpoints = self.manual_config.get("endpoints", [])
        manual_map = {ep["model"]: ep for ep in manual_endpoints}

        # Generate routes for discovered models
        for model_name in models_to_expose:
            manual_settings = manual_map.get(model_name, {})
            self._create_auto_route(model_name, manual_settings)

        # Also add any manual endpoints explicitly defined (even without production tag or api_ prefix)
        for ep in manual_endpoints:
            if ep["model"] not in models_to_expose:
                self._create_auto_route(ep["model"], ep)

    def _get_hierarchical_tags(self, dbt_tags: List[str]) -> List[str]:
        """
        Convert dbt tags into Swagger UI sections.
        
        Filters out 'production', tier tags, and other system tags, then uses the FIRST
        remaining tag as the main category. This groups all related endpoints
        together (e.g., all Execution endpoints under one "Execution" section).
        
        Examples:
            ["production", "execution", "transactions"] -> ["Execution"]
            ["production", "consensus", "validators", "active"] -> ["Consensus"]
            ["production", "financial"] -> ["Financial"]
            ["production"] -> ["General"]
        """
        # Tags to exclude from hierarchy (including tier tags)
        system_tags = {
            'production',
            'daily',
            'hourly',
            'weekly',
            'monthly',
            'view',
            'table',
            'incremental',
            'staging',
            'intermediate'
        }

        # Filter out system tags and tier tags (tier0, tier1, etc.)
        hierarchy_tags = []
        for t in dbt_tags:
            t_lower = t.lower()
            if t_lower in system_tags:
                continue
            if re.match(r'^tier\d+$', t_lower):
                continue
            hierarchy_tags.append(t)

        if not hierarchy_tags:
            return ["General"]

        # Use only the first tag as the main category
        main_section = hierarchy_tags[0].replace("_", " ").title()
        return [main_section]

    def _get_required_tier(self, dbt_tags: List[str]) -> str:
        """
        Extract the tier requirement from dbt tags.
        
        Looks for tags matching 'tier0', 'tier1', 'tier2', etc.
        Returns the DEFAULT_ENDPOINT_TIER if no tier tag is found.
        
        Examples:
            ["production", "execution", "tier1"] -> "tier1"
            ["production", "consensus"] -> settings.DEFAULT_ENDPOINT_TIER
            ["tier2", "production", "financial"] -> "tier2"
        """
        for tag in dbt_tags:
            if re.match(r'^tier\d+$', tag.lower()):
                return tag.lower()
        
        return settings.DEFAULT_ENDPOINT_TIER

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

        # --- Hierarchical Tag Grouping ---
        # Manual override takes precedence, otherwise derive from dbt tags
        if override.get("tags"):
            api_tags = override.get("tags")
        else:
            api_tags = self._get_hierarchical_tags(dbt_tags)

        # --- Tier Access Requirement ---
        required_tier = override.get("tier", self._get_required_tier(dbt_tags))

        # --- Auto-Detect Parameters ---
        allowed_params = override.get("parameters", [])

        # If no manual params, detect them from columns
        if not allowed_params:
            # 1. Date Filters
            date_cols = [
                c for c in columns
                if 'Date' in columns[c] or 'Time' in columns[c]
                or c in ['date', 'timestamp', 'block_timestamp']
            ]
            if date_cols:
                main_date_col = date_cols[0]
                allowed_params.append({
                    "name": "start_date",
                    "column": main_date_col,
                    "operator": ">=",
                    "type": "date"
                })
                allowed_params.append({
                    "name": "end_date",
                    "column": main_date_col,
                    "operator": "<=",
                    "type": "date"
                })

            # 2. Address Filters
            if 'address' in columns:
                allowed_params.append({
                    "name": "address",
                    "column": "address",
                    "operator": "ILIKE",
                    "type": "string"
                })

            # 3. Common IDs
            for col in ['project', 'sector', 'label', 'status']:
                if col in columns:
                    allowed_params.append({
                        "name": col,
                        "column": col,
                        "operator": "=",
                        "type": "string"
                    })

        # Default ordering (Date DESC is usually best for timeseries)
        order_by = override.get("order_by")
        if not order_by:
            date_cols = [
                c for c in columns
                if 'Date' in columns[c] or 'Time' in columns[c]
                or c in ['date', 'timestamp']
            ]
            if date_cols:
                order_by = f"{date_cols[0]} DESC"

        # --- Route Handler ---
        table_name = manifest.get_table_name(model_name)
        # Capture required_tier in closure
        endpoint_required_tier = required_tier
        endpoint_path = url_path

        async def dynamic_handler(
            request: Request,
            limit: int = Query(100, ge=1, le=5000),
            offset: int = Query(0, ge=0),
            user_info: Dict[str, Any] = Depends(get_api_key)
        ):
            # Check tier-based access control
            check_tier_access(user_info, endpoint_required_tier, endpoint_path)
            
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
        # Add column info and tier requirement to description
        col_doc = "\n".join([f"- **{k}**: {v}" for k, v in columns.items()])
        tier_doc = f"**Required Access:** `{required_tier}`"
        full_desc = f"{tier_doc}\n\n{dbt_node.get('description', '')}\n\n**Columns:**\n{col_doc}"

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