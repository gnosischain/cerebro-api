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

    def _extract_api_resource(self, dbt_tags: List[str]) -> Optional[str]:
        """
        Extract resource name from 'api:xyz' tag.
        
        Examples:
            ["production", "consensus", "api:blob_commitments"] -> "blob_commitments"
            ["production", "execution", "api:gas_used"] -> "gas_used"
            ["production", "consensus"] -> None
        """
        for tag in dbt_tags:
            if tag.startswith("api:"):
                resource = tag[4:].strip()
                if resource:
                    return resource
        return None

    def _extract_granularity(self, dbt_tags: List[str]) -> Optional[str]:
        """
        Extract granularity from 'granularity:xyz' tag.
        
        Examples:
            ["production", "granularity:daily"] -> "daily"
            ["production", "granularity:latest"] -> "latest"
            ["production", "consensus"] -> None
        """
        for tag in dbt_tags:
            if tag.startswith("granularity:"):
                granularity = tag[12:].strip().lower()
                if granularity:
                    return granularity
        return None

    def _extract_category(self, dbt_tags: List[str]) -> str:
        """
        Extract primary category (first non-system, non-prefixed tag).
        
        Examples:
            ["production", "consensus", "tier1", "api:blob"] -> "consensus"
            ["production", "execution", "api:gas"] -> "execution"
            ["production", "api:test"] -> "general"
        """
        system_tags = {
            'production', 'view', 'table', 'incremental',
            'staging', 'intermediate',
            # Granularity values (in case used as standalone tags)
            'daily', 'weekly', 'monthly', 'hourly',
            'latest', 'in_ranges', 'last_30d', 'last_7d', 'all_time'
        }
        
        for tag in dbt_tags:
            tag_lower = tag.lower()
            # Skip system tags
            if tag_lower in system_tags:
                continue
            # Skip tier tags
            if re.match(r'^tier\d+$', tag_lower):
                continue
            # Skip prefixed tags (api:, granularity:)
            if ':' in tag:
                continue
            return tag_lower
        
        return "general"

    def _build_url_path(self, model_name: str, dbt_tags: List[str], override: dict) -> str:
        """
        Build URL path from tags.
        
        Path structure: /{category}/{resource}/{granularity?}
        
        Examples:
            tags=["production", "consensus", "api:blob_commitments", "granularity:daily"]
            -> /consensus/blob_commitments/daily
            
            tags=["production", "execution", "api:transactions"]
            -> /execution/transactions
        """
        # Manual override takes precedence
        if override.get("path"):
            return override["path"]
        
        # Extract components from tags
        api_resource = self._extract_api_resource(dbt_tags)
        
        if not api_resource:
            # This shouldn't happen if _build_routes filters correctly,
            # but provide a fallback just in case
            return None
        
        category = self._extract_category(dbt_tags)
        granularity = self._extract_granularity(dbt_tags)
        
        # Build: /{category}/{resource}/{granularity?}
        path_parts = [category, api_resource]
        if granularity:
            path_parts.append(granularity)
        
        return "/" + "/".join(path_parts)

    def _build_routes(self):
        """
        Build routes for models that have BOTH:
        1. The 'production' tag
        2. An 'api:' tag defining the resource name
        
        Models without an 'api:' tag are NOT exposed, even if they start with 'api_'.
        """
        models_to_expose = set()

        # Auto-discovery: Only expose models with 'production' AND 'api:' tags
        for model_name in manifest.get_all_models():
            dbt_tags = manifest.get_tags(model_name)
            
            # Must have 'production' tag
            if "production" not in dbt_tags:
                continue
            
            # Must have an 'api:' tag
            if self._extract_api_resource(dbt_tags) is not None:
                models_to_expose.add(model_name)

        print(f"📡 Discovered {len(models_to_expose)} models with 'production' + 'api:' tags")

        # Manual overrides from config
        manual_endpoints = self.manual_config.get("endpoints", [])
        manual_map = {ep["model"]: ep for ep in manual_endpoints}

        # Generate routes for discovered models
        for model_name in models_to_expose:
            manual_settings = manual_map.get(model_name, {})
            self._create_auto_route(model_name, manual_settings)

        # Also add any manual endpoints explicitly defined (even without api: tag)
        for ep in manual_endpoints:
            if ep["model"] not in models_to_expose:
                self._create_auto_route(ep["model"], ep)

    def _get_hierarchical_tags(self, dbt_tags: List[str]) -> List[str]:
        """
        Convert dbt tags into Swagger UI sections.
        
        Filters out 'production', tier tags, prefixed tags (api:, granularity:),
        and other system tags, then uses the FIRST remaining tag as the main category.
        
        Examples:
            ["production", "consensus", "api:blob", "granularity:daily"] -> ["Consensus"]
            ["production", "execution", "tier1", "api:gas"] -> ["Execution"]
            ["production", "api:test"] -> ["General"]
        """
        # Tags to exclude from hierarchy
        system_tags = {
            'production',
            'view',
            'table',
            'incremental',
            'staging',
            'intermediate',
            # Granularity values (in case used as standalone tags)
            'daily',
            'weekly',
            'monthly',
            'hourly',
            'latest',
            'in_ranges',
            'last_30d',
            'last_7d',
            'all_time'
        }

        # Filter out system tags, tier tags, and prefixed tags
        hierarchy_tags = []
        for t in dbt_tags:
            t_lower = t.lower()
            # Skip system tags
            if t_lower in system_tags:
                continue
            # Skip tier tags (tier0, tier1, etc.)
            if re.match(r'^tier\d+$', t_lower):
                continue
            # Skip prefixed tags (api:, granularity:)
            if ':' in t:
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

        # --- Build URL Path from Tags ---
        url_path = self._build_url_path(model_name, dbt_tags, override)
        
        if not url_path:
            print(f"⚠️ Skipping {model_name}: no valid URL path could be generated")
            return

        # --- Generate Summary ---
        api_resource = self._extract_api_resource(dbt_tags)
        granularity = self._extract_granularity(dbt_tags)
        
        if override.get("summary"):
            summary = override["summary"]
        elif api_resource:
            # Build summary from resource and granularity
            summary_parts = [api_resource.replace("_", " ").title()]
            if granularity:
                summary_parts.append(f"({granularity})")
            summary = " ".join(summary_parts)
        else:
            summary = model_name.replace("_", " ").title()

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

        print(f"  ✅ {url_path} -> {model_name} [{required_tier}]")


router = DynamicRouter().router