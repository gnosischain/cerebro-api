import json
import os
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import yaml
from fastapi import APIRouter, Body, Depends, HTTPException, Request
from pydantic import ConfigDict, Field, create_model

from app.api_metadata import (
    ApiEndpointSpec,
    ApiMetadataError,
    ApiPaginationSpec,
    ApiParamSpec,
    ApiSortSpec,
    build_api_behavior,
    extract_raw_api_metadata,
)
from app.config import settings
from app.database import ClickHouseClient
from app.manifest import manifest
from app.security import check_tier_access, get_api_key, get_optional_api_key

LEGACY_PARAM_ERROR = (
    "This endpoint does not declare API parameters. "
    "Add meta.api to the dbt model to enable filters or pagination."
)
MANUAL_OVERRIDE_KEYS = {"model", "path", "summary", "tags", "tier"}
_GRANULARITY_ORDER = [
    "latest",
    "daily",
    "weekly",
    "monthly",
    "last_7d",
    "last_30d",
    "in_ranges",
    "all_time",
]
_GRANULARITY_RANK = {value: index + 1 for index, value in enumerate(_GRANULARITY_ORDER)}


@dataclass
class ParsedRequest:
    filters: Dict[str, Any]
    provided_business_filters: Set[str]
    limit: Optional[int] = None
    offset: Optional[int] = None


class DynamicRouter:
    def __init__(self, previous_specs: Optional[Dict[str, ApiEndpointSpec]] = None):
        self.router = APIRouter()
        self.previous_specs = previous_specs or {}
        self.valid_specs: Dict[str, ApiEndpointSpec] = {}
        self.warnings: List[str] = []
        self.manual_config = self._load_manual_config()
        self._build_routes()

    def _load_manual_config(self) -> Dict[str, Any]:
        if os.path.exists(settings.API_CONFIG_PATH):
            with open(settings.API_CONFIG_PATH, "r") as f:
                data = yaml.safe_load(f)
                return data or {}
        return {}

    def _sanitize_manual_override(self, model_name: str, raw_override: Any) -> Dict[str, Any]:
        if raw_override is None:
            return {}
        if not isinstance(raw_override, dict):
            raise ApiMetadataError(f"{model_name}: api_config override must be an object")

        unsupported = sorted(set(raw_override.keys()) - MANUAL_OVERRIDE_KEYS)
        if unsupported:
            warning = (
                f"⚠️ {model_name}: ignoring unsupported api_config override keys: "
                f"{', '.join(unsupported)}"
            )
            print(warning)
            self.warnings.append(warning)

        override = {key: raw_override[key] for key in MANUAL_OVERRIDE_KEYS if key in raw_override}

        if "path" in override and (not isinstance(override["path"], str) or not override["path"].strip()):
            raise ApiMetadataError(f"{model_name}: api_config.path must be a non-empty string")
        if "summary" in override and not isinstance(override["summary"], str):
            raise ApiMetadataError(f"{model_name}: api_config.summary must be a string")
        if "tier" in override and not isinstance(override["tier"], str):
            raise ApiMetadataError(f"{model_name}: api_config.tier must be a string")

        if "tags" in override:
            tags = override["tags"]
            if not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags):
                raise ApiMetadataError(f"{model_name}: api_config.tags must be a list of strings")

        return override

    def _extract_api_resource(self, dbt_tags: List[str]) -> Optional[str]:
        for tag in dbt_tags:
            if tag.startswith("api:"):
                resource = tag[4:].strip()
                if resource:
                    return resource
        return None

    def _extract_granularity(self, dbt_tags: List[str]) -> Optional[str]:
        for tag in dbt_tags:
            if tag.startswith("granularity:"):
                granularity = tag[12:].strip().lower()
                if granularity:
                    return granularity
        return None

    def _extract_category(self, dbt_tags: List[str]) -> str:
        system_tags = {
            "production",
            "view",
            "table",
            "incremental",
            "staging",
            "intermediate",
            "daily",
            "weekly",
            "monthly",
            "hourly",
            "latest",
            "in_ranges",
            "last_30d",
            "last_7d",
            "all_time",
        }

        for tag in dbt_tags:
            tag_lower = tag.lower()
            if tag_lower in system_tags:
                continue
            if re.match(r"^tier\d+$", tag_lower):
                continue
            if ":" in tag:
                continue
            return tag_lower
        return "general"

    def _build_url_path(
        self,
        model_name: str,
        dbt_tags: List[str],
        override: Dict[str, Any],
    ) -> Optional[str]:
        if override.get("path"):
            return override["path"]

        api_resource = self._extract_api_resource(dbt_tags)
        if not api_resource:
            return None

        category = self._extract_category(dbt_tags)
        granularity = self._extract_granularity(dbt_tags)

        path_parts = [category, api_resource]
        if granularity:
            path_parts.append(granularity)
        return "/" + "/".join(path_parts)

    def _get_hierarchical_tags(self, dbt_tags: List[str]) -> List[str]:
        system_tags = {
            "production",
            "view",
            "table",
            "incremental",
            "staging",
            "intermediate",
            "daily",
            "weekly",
            "monthly",
            "hourly",
            "latest",
            "in_ranges",
            "last_30d",
            "last_7d",
            "all_time",
        }

        hierarchy_tags = []
        for tag in dbt_tags:
            tag_lower = tag.lower()
            if tag_lower in system_tags:
                continue
            if re.match(r"^tier\d+$", tag_lower):
                continue
            if ":" in tag:
                continue
            hierarchy_tags.append(tag)

        if not hierarchy_tags:
            return ["General"]
        return [hierarchy_tags[0].replace("_", " ").title()]

    def _get_required_tier(self, dbt_tags: List[str]) -> str:
        for tag in dbt_tags:
            if re.match(r"^tier\d+$", tag.lower()):
                return tag.lower()
        return settings.DEFAULT_ENDPOINT_TIER

    def _get_summary(
        self,
        model_name: str,
        dbt_tags: List[str],
        override: Dict[str, Any],
    ) -> str:
        if override.get("summary"):
            return override["summary"]

        api_resource = self._extract_api_resource(dbt_tags)
        granularity = self._extract_granularity(dbt_tags)
        if api_resource:
            parts = [api_resource.replace("_", " ").title()]
            if granularity:
                parts.append(f"({granularity})")
            return " ".join(parts)
        return model_name.replace("_", " ").title()

    def _build_filter_doc(self, parameters: List[ApiParamSpec]) -> str:
        return "\n".join(
            [
                f"- **{param.name}**: {param.operator} on `{param.column}`"
                + (f" ({param.description})" if param.description else "")
                for param in parameters
            ]
        )

    def _build_pagination_doc(self, pagination: ApiPaginationSpec) -> str:
        if not pagination.enabled:
            return ""
        return "\n".join(
            [
                f"- **limit**: default `{pagination.default_limit}`, max `{pagination.max_limit}`",
                "- **offset**: default `0`",
            ]
        )

    def _build_sort_doc(self, sort: List[ApiSortSpec]) -> str:
        if not sort:
            return ""
        return "\n".join([f"- `{item.column} {item.direction}`" for item in sort])

    def _build_description(
        self,
        dbt_node: Dict[str, Any],
        spec_tier: str,
        metadata_enabled: bool,
        parameters: List[ApiParamSpec],
        pagination: ApiPaginationSpec,
        sort: List[ApiSortSpec],
        columns: Dict[str, str],
    ) -> str:
        auth_note = " (no API key required)" if spec_tier == "tier0" else ""
        tier_doc = f"**Required Access:** `{spec_tier}`{auth_note}"
        column_doc = "\n".join([f"- **{name}**: {data_type}" for name, data_type in columns.items()])

        description_parts = [tier_doc, dbt_node.get("description", "")]
        if metadata_enabled and parameters:
            description_parts.append(f"**Declared Filters:**\n{self._build_filter_doc(parameters)}")
        if metadata_enabled and pagination.enabled:
            description_parts.append(f"**Pagination:**\n{self._build_pagination_doc(pagination)}")
        if metadata_enabled and sort:
            description_parts.append(f"**Sort:**\n{self._build_sort_doc(sort)}")
        description_parts.append(f"**Columns:**\n{column_doc}")

        return "\n\n".join(part for part in description_parts if part)

    def _build_endpoint_spec(self, model_name: str, override: Dict[str, Any]) -> ApiEndpointSpec:
        dbt_node = manifest.get_model(model_name)
        if not dbt_node:
            raise ApiMetadataError(f"{model_name}: model not found in manifest")

        columns = manifest.get_columns(model_name)
        dbt_tags = manifest.get_tags(model_name)
        path = self._build_url_path(model_name, dbt_tags, override)
        if not path:
            raise ApiMetadataError(f"{model_name}: no valid API path could be generated")

        raw_api_exists, raw_api = extract_raw_api_metadata(dbt_node)
        behavior = build_api_behavior(model_name, columns, raw_api_exists, raw_api)

        tags = override.get("tags") or self._get_hierarchical_tags(dbt_tags)
        tier = override.get("tier", self._get_required_tier(dbt_tags))
        summary = self._get_summary(model_name, dbt_tags, override)
        description = self._build_description(
            dbt_node=dbt_node,
            spec_tier=tier,
            metadata_enabled=behavior.metadata_enabled,
            parameters=behavior.parameters,
            pagination=behavior.pagination,
            sort=behavior.sort,
            columns=columns,
        )

        return ApiEndpointSpec(
            model_name=model_name,
            table_name=manifest.get_table_name(model_name),
            path=path,
            summary=summary,
            tags=tags,
            tier=tier,
            methods=behavior.methods,
            allow_unfiltered=behavior.allow_unfiltered,
            require_any_of=behavior.require_any_of,
            parameters=behavior.parameters,
            pagination=behavior.pagination,
            sort=behavior.sort,
            description=description,
            metadata_enabled=behavior.metadata_enabled,
        )

    def _parse_route_path(self, path: str) -> Optional[Tuple[str, str, Optional[str]]]:
        parts = [part.strip().lower() for part in path.strip("/").split("/") if part.strip()]
        if len(parts) == 2:
            category, resource = parts
            return category, resource, None
        if len(parts) == 3:
            category, resource, granularity = parts
            return category, resource, granularity
        return None

    def _spec_sort_key(self, spec: ApiEndpointSpec) -> Tuple[int, str, str, int, str, str]:
        parsed = self._parse_route_path(spec.path)
        if parsed is None:
            # Custom or unexpected paths fall back to lexical path ordering.
            return (1, "", "", 0, "", spec.path)

        category, resource, granularity = parsed
        if granularity is None:
            granularity_rank = 0
            granularity_key = ""
        else:
            granularity_rank = _GRANULARITY_RANK.get(granularity, len(_GRANULARITY_ORDER) + 1)
            granularity_key = granularity

        return (0, category, resource, granularity_rank, granularity_key, spec.path)

    def _build_routes(self) -> None:
        models_to_expose = set()
        for model_name in manifest.get_all_models():
            tags = manifest.get_tags(model_name)
            if "production" not in tags:
                continue
            if self._extract_api_resource(tags) is not None:
                models_to_expose.add(model_name)

        print(f"📡 Discovered {len(models_to_expose)} models with 'production' + 'api:' tags")

        manual_endpoints = self.manual_config.get("endpoints", []) or []
        manual_map: Dict[str, Dict[str, Any]] = {}
        for entry in manual_endpoints:
            if not isinstance(entry, dict) or "model" not in entry:
                warning = "⚠️ Invalid api_config endpoint entry: missing model"
                print(warning)
                self.warnings.append(warning)
                continue
            model_name = entry["model"]
            if not isinstance(model_name, str) or not model_name.strip():
                warning = "⚠️ Invalid api_config endpoint entry: model must be a non-empty string"
                print(warning)
                self.warnings.append(warning)
                continue
            try:
                manual_map[model_name] = self._sanitize_manual_override(model_name, entry)
            except ApiMetadataError as exc:
                warning = f"⚠️ {exc}. Override ignored."
                print(warning)
                self.warnings.append(warning)

        all_models = models_to_expose | set(manual_map.keys())
        specs_to_register: List[ApiEndpointSpec] = []

        for model_name in sorted(all_models):
            dbt_node = manifest.get_model(model_name)
            if not dbt_node:
                warning = f"⚠️ Skipping {model_name}: model not found in manifest"
                print(warning)
                self.warnings.append(warning)
                continue

            override = manual_map.get(model_name, {})
            try:
                spec = self._build_endpoint_spec(model_name, override)
            except ApiMetadataError as exc:
                cached_spec = self.previous_specs.get(model_name)
                if cached_spec:
                    warning = f"⚠️ {exc}. Reusing last known good endpoint for {model_name}."
                    print(warning)
                    self.warnings.append(warning)
                    spec = cached_spec
                else:
                    warning = f"⚠️ {exc}. Endpoint skipped."
                    print(warning)
                    self.warnings.append(warning)
                    continue

            self.valid_specs[model_name] = spec
            specs_to_register.append(spec)

        for spec in sorted(specs_to_register, key=self._spec_sort_key):
            self._register_endpoint(spec)

    def _get_auth_dependency(self, required_tier: str):
        return get_optional_api_key if required_tier == "tier0" else get_api_key

    def _normalize_scalar_value(self, param: ApiParamSpec, value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, list):
            raise HTTPException(status_code=400, detail=f"Parameter '{param.name}' expects a single value.")
        value = str(value).strip()
        if not value:
            return None
        if param.case == "lower":
            return value.lower()
        if param.case == "upper":
            return value.upper()
        return value

    def _normalize_list_value(self, param: ApiParamSpec, value: Any) -> Optional[Tuple[str, ...]]:
        if value is None:
            return None
        raw_items = value if isinstance(value, list) else [value]
        items: List[str] = []
        for raw_item in raw_items:
            if raw_item is None:
                continue
            if not isinstance(raw_item, str):
                raw_item = str(raw_item)
            for piece in raw_item.split(","):
                item = piece.strip()
                if not item:
                    continue
                if param.case == "lower":
                    item = item.lower()
                elif param.case == "upper":
                    item = item.upper()
                items.append(item)

        if not items:
            return None

        if param.max_items is not None and len(items) > param.max_items:
            raise HTTPException(
                status_code=400,
                detail=f"Parameter '{param.name}' allows at most {param.max_items} values.",
            )
        return tuple(items)

    def _parse_filter_value(self, param: ApiParamSpec, raw_value: Any) -> Any:
        if param.type == "string_list":
            return self._normalize_list_value(param, raw_value)
        return self._normalize_scalar_value(param, raw_value)

    def _parse_int_value(self, value: Any, field_name: str, min_value: int) -> int:
        if isinstance(value, bool):
            raise HTTPException(status_code=400, detail=f"'{field_name}' must be an integer.")

        if isinstance(value, str):
            value = value.strip()
            if not value:
                raise HTTPException(status_code=400, detail=f"'{field_name}' must be an integer.")

        try:
            parsed = int(value)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail=f"'{field_name}' must be an integer.")

        if parsed < min_value:
            raise HTTPException(
                status_code=400,
                detail=f"'{field_name}' must be >= {min_value}.",
            )
        return parsed

    def _parse_get_pagination(self, spec: ApiEndpointSpec, request: Request) -> Tuple[Optional[int], Optional[int]]:
        if not spec.pagination.enabled:
            return None, None

        limit_raw = request.query_params.get("limit")
        if limit_raw is None:
            limit = spec.pagination.default_limit
        else:
            limit = self._parse_int_value(limit_raw, "limit", 1)

        if limit is None:
            raise HTTPException(status_code=500, detail="Endpoint pagination default limit is not configured.")

        max_limit = spec.pagination.max_limit
        if max_limit is not None and limit > max_limit:
            raise HTTPException(
                status_code=400,
                detail=f"'limit' must be <= {max_limit}.",
            )

        offset_raw = request.query_params.get("offset")
        offset = 0 if offset_raw is None else self._parse_int_value(offset_raw, "offset", 0)
        return limit, offset

    def _parse_post_pagination(
        self,
        spec: ApiEndpointSpec,
        payload: Dict[str, Any],
    ) -> Tuple[Optional[int], Optional[int]]:
        if not spec.pagination.enabled:
            return None, None

        if "limit" in payload:
            limit = self._parse_int_value(payload["limit"], "limit", 1)
        else:
            limit = spec.pagination.default_limit

        if limit is None:
            raise HTTPException(status_code=500, detail="Endpoint pagination default limit is not configured.")

        max_limit = spec.pagination.max_limit
        if max_limit is not None and limit > max_limit:
            raise HTTPException(
                status_code=400,
                detail=f"'limit' must be <= {max_limit}.",
            )

        if "offset" in payload:
            offset = self._parse_int_value(payload["offset"], "offset", 0)
        else:
            offset = 0

        return limit, offset

    def _parse_get_request(
        self,
        spec: ApiEndpointSpec,
        request: Request,
    ) -> ParsedRequest:
        if not spec.metadata_enabled:
            if request.query_params:
                raise HTTPException(status_code=400, detail=LEGACY_PARAM_ERROR)
            return ParsedRequest(filters={}, provided_business_filters=set(), limit=None, offset=None)

        allowed_names = {param.name for param in spec.parameters}
        if spec.pagination.enabled:
            allowed_names.update({"limit", "offset"})

        unexpected_params = sorted({key for key in request.query_params.keys() if key not in allowed_names})
        if unexpected_params:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported query parameters: {', '.join(unexpected_params)}",
            )

        filters: Dict[str, Any] = {}
        provided_business_filters: Set[str] = set()
        for param in spec.parameters:
            if param.type == "string_list":
                raw_value: Any = request.query_params.getlist(param.name)
            else:
                raw_value = request.query_params.get(param.name)

            parsed_value = self._parse_filter_value(param, raw_value)
            if parsed_value is None:
                continue

            filters[param.name] = parsed_value
            provided_business_filters.add(param.name)

        limit, offset = self._parse_get_pagination(spec, request)
        return ParsedRequest(
            filters=filters,
            provided_business_filters=provided_business_filters,
            limit=limit,
            offset=offset,
        )

    def _parse_post_request(
        self,
        spec: ApiEndpointSpec,
        payload: Dict[str, Any],
    ) -> ParsedRequest:
        allowed_names = {param.name for param in spec.parameters}
        if spec.pagination.enabled:
            allowed_names.update({"limit", "offset"})

        unexpected_fields = sorted(set(payload.keys()) - allowed_names)
        if unexpected_fields:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported body fields: {', '.join(unexpected_fields)}",
            )

        filters: Dict[str, Any] = {}
        provided_business_filters: Set[str] = set()
        for param in spec.parameters:
            parsed_value = self._parse_filter_value(param, payload.get(param.name))
            if parsed_value is None:
                continue

            filters[param.name] = parsed_value
            provided_business_filters.add(param.name)

        limit, offset = self._parse_post_pagination(spec, payload)
        return ParsedRequest(
            filters=filters,
            provided_business_filters=provided_business_filters,
            limit=limit,
            offset=offset,
        )

    async def _read_json_payload(self, request: Request) -> Dict[str, Any]:
        raw = await request.body()
        if not raw:
            return {}

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Request body must be valid JSON.")

        if payload is None:
            return {}
        if not isinstance(payload, dict):
            raise HTTPException(status_code=400, detail="Request body must be a JSON object.")
        return payload

    def _enforce_filter_policy(self, spec: ApiEndpointSpec, parsed_request: ParsedRequest) -> None:
        if not spec.metadata_enabled:
            return

        if spec.require_any_of:
            provided = parsed_request.provided_business_filters
            if not provided.intersection(spec.require_any_of):
                required = ", ".join(spec.require_any_of)
                raise HTTPException(
                    status_code=400,
                    detail=f"At least one of [{required}] is required for this endpoint.",
                )

        if not spec.allow_unfiltered and not parsed_request.provided_business_filters:
            raise HTTPException(
                status_code=400,
                detail="At least one business filter is required for this endpoint.",
            )

    def _get_sql_column_expr(self, param: ApiParamSpec) -> str:
        if param.case == "lower":
            return f"lower({param.column})"
        if param.case == "upper":
            return f"upper({param.column})"
        return param.column

    def _build_order_by_clause(self, spec: ApiEndpointSpec) -> str:
        if not spec.sort:
            return ""
        return ", ".join([f"{item.column} {item.direction}" for item in spec.sort])

    def _execute_dynamic_query(
        self,
        spec: ApiEndpointSpec,
        parsed_request: ParsedRequest,
        user_info: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        check_tier_access(user_info, spec.tier, spec.path)
        self._enforce_filter_policy(spec, parsed_request)

        sql = f"SELECT * FROM {spec.table_name}"
        where_parts = []
        query_params: Dict[str, Any] = {}

        for param in spec.parameters:
            if param.name not in parsed_request.filters:
                continue

            value = parsed_request.filters[param.name]
            key = f"p_{param.name}"
            column_expr = self._get_sql_column_expr(param)

            if param.operator == "IN":
                where_parts.append(f"{column_expr} IN %({key})s")
                query_params[key] = tuple(value)
            elif "LIKE" in param.operator:
                where_parts.append(f"{column_expr} {param.operator} %({key})s")
                query_params[key] = value if "%" in value else f"%{value}%"
            else:
                where_parts.append(f"{column_expr} {param.operator} %({key})s")
                query_params[key] = value

        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)

        order_by = self._build_order_by_clause(spec)
        if order_by:
            sql += f" ORDER BY {order_by}"

        if spec.pagination.enabled:
            if parsed_request.limit is None or parsed_request.offset is None:
                raise HTTPException(status_code=500, detail="Pagination values are missing.")
            sql += " LIMIT %(limit)s OFFSET %(offset)s"
            query_params["limit"] = parsed_request.limit
            query_params["offset"] = parsed_request.offset

        try:
            return ClickHouseClient.query(sql, query_params)
        except HTTPException:
            raise
        except Exception as exc:
            raise HTTPException(status_code=500, detail=str(exc))

    def _build_get_openapi_parameters(self, spec: ApiEndpointSpec) -> List[Dict[str, Any]]:
        parameters = []
        for param in spec.parameters:
            schema: Dict[str, Any]
            parameter_doc: Dict[str, Any] = {
                "name": param.name,
                "in": "query",
                "required": False,
                "description": param.description or "",
            }

            if param.type == "date":
                schema = {"type": "string", "format": "date"}
            elif param.type == "string_list":
                schema = {"type": "array", "items": {"type": "string"}}
                if param.max_items:
                    schema["maxItems"] = param.max_items
                parameter_doc["style"] = "form"
                parameter_doc["explode"] = True
                description = parameter_doc["description"] or "List filter"
                parameter_doc["description"] = (
                    f"{description}. Accepts repeated query params or CSV."
                )
            else:
                schema = {"type": "string"}

            parameter_doc["schema"] = schema
            parameters.append(parameter_doc)

        if spec.pagination.enabled:
            parameters.append(
                {
                    "name": "limit",
                    "in": "query",
                    "required": False,
                    "description": "Row limit",
                    "schema": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": spec.pagination.max_limit,
                        "default": spec.pagination.default_limit,
                    },
                }
            )
            parameters.append(
                {
                    "name": "offset",
                    "in": "query",
                    "required": False,
                    "description": "Row offset",
                    "schema": {"type": "integer", "minimum": 0, "default": 0},
                }
            )

        return parameters

    def _create_post_body_model(self, spec: ApiEndpointSpec):
        model_fields: Dict[str, Any] = {}
        for param in spec.parameters:
            field_kwargs: Dict[str, Any] = {"default": None}
            if param.description:
                field_kwargs["description"] = param.description
            if param.type == "string_list" and param.max_items:
                field_kwargs["max_items"] = param.max_items

            if param.type == "string_list":
                annotation = Optional[Union[List[str], str]]
            else:
                annotation = Optional[str]

            model_fields[param.name] = (annotation, Field(**field_kwargs))

        if spec.pagination.enabled:
            model_fields["limit"] = (
                Optional[int],
                Field(
                    default=spec.pagination.default_limit,
                    ge=1,
                    le=spec.pagination.max_limit,
                    description="Row limit",
                ),
            )
            model_fields["offset"] = (
                Optional[int],
                Field(default=0, ge=0, description="Row offset"),
            )

        safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", spec.model_name)
        return create_model(
            f"{safe_name}_PostBody",
            __config__=ConfigDict(extra="allow"),
            **model_fields,
        )

    def _register_endpoint(self, spec: ApiEndpointSpec) -> None:
        auth_dependency = self._get_auth_dependency(spec.tier)

        async def get_handler(
            request: Request,
            user_info: Dict[str, Any] = Depends(auth_dependency),
        ):
            parsed_request = self._parse_get_request(spec, request)
            return self._execute_dynamic_query(spec, parsed_request, user_info)

        get_handler.__doc__ = spec.description

        openapi_parameters = self._build_get_openapi_parameters(spec)
        openapi_extra = {"parameters": openapi_parameters} if openapi_parameters else None

        if "GET" in spec.methods:
            self.router.add_api_route(
                path=spec.path,
                endpoint=get_handler,
                methods=["GET"],
                summary=spec.summary,
                description=spec.description,
                tags=spec.tags,
                name=f"{spec.model_name}_get",
                openapi_extra=openapi_extra,
            )

        if "POST" in spec.methods:
            body_model = self._create_post_body_model(spec)
            body_type = Optional[body_model]

            async def post_handler(
                request: Request,
                _body: body_type = Body(default=None),
                user_info: Dict[str, Any] = Depends(auth_dependency),
            ):
                if request.query_params:
                    raise HTTPException(
                        status_code=400,
                        detail="POST endpoints accept filter and pagination fields in JSON body only.",
                    )

                payload = await self._read_json_payload(request)
                parsed_request = self._parse_post_request(spec, payload)
                return self._execute_dynamic_query(spec, parsed_request, user_info)

            post_handler.__doc__ = spec.description
            self.router.add_api_route(
                path=spec.path,
                endpoint=post_handler,
                methods=["POST"],
                summary=f"{spec.summary} (POST)",
                description=spec.description,
                tags=spec.tags,
                name=f"{spec.model_name}_post",
            )

        methods = ", ".join(spec.methods)
        print(f"  ✅ {spec.path} -> {spec.model_name} [{spec.tier}] ({methods})")


def build_router(
    previous_specs: Optional[Dict[str, ApiEndpointSpec]] = None,
) -> Tuple[APIRouter, Dict[str, ApiEndpointSpec], List[str]]:
    dynamic_router = DynamicRouter(previous_specs=previous_specs)
    return dynamic_router.router, dynamic_router.valid_specs, dynamic_router.warnings
