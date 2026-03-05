from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


ALLOWED_METHODS = {"GET", "POST"}
ALLOWED_OPERATORS = {"=", ">=", "<=", "ILIKE", "IN"}
ALLOWED_TYPES = {"string", "date", "string_list"}
ALLOWED_CASE_MODES = {"lower", "upper"}
ALLOWED_SORT_DIRECTIONS = {"ASC", "DESC"}


class ApiMetadataError(ValueError):
    pass


@dataclass(frozen=True)
class ApiParamSpec:
    name: str
    column: str
    operator: str
    type: str
    description: Optional[str] = None
    case: Optional[str] = None
    max_items: Optional[int] = None
    source: str = "metadata"


@dataclass(frozen=True)
class ApiPaginationSpec:
    enabled: bool
    default_limit: Optional[int] = None
    max_limit: Optional[int] = None


@dataclass(frozen=True)
class ApiSortSpec:
    column: str
    direction: str


@dataclass(frozen=True)
class ApiEndpointSpec:
    model_name: str
    table_name: str
    path: str
    summary: str
    tags: List[str]
    tier: str
    methods: List[str]
    allow_unfiltered: bool
    require_any_of: List[str]
    parameters: List[ApiParamSpec]
    pagination: ApiPaginationSpec
    sort: List[ApiSortSpec]
    description: str
    metadata_enabled: bool = False


@dataclass(frozen=True)
class ApiBehaviorConfig:
    metadata_enabled: bool
    methods: List[str]
    allow_unfiltered: bool
    require_any_of: List[str]
    parameters: List[ApiParamSpec]
    pagination: ApiPaginationSpec
    sort: List[ApiSortSpec]


def extract_raw_api_metadata(dbt_node: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
    config_meta = dbt_node.get("config", {}).get("meta")
    if isinstance(config_meta, dict) and "api" in config_meta:
        raw_api = config_meta.get("api")
        if raw_api is None:
            return True, {}
        if not isinstance(raw_api, dict):
            raise ApiMetadataError("config.meta.api must be an object")
        return True, raw_api

    model_meta = dbt_node.get("meta")
    if isinstance(model_meta, dict) and "api" in model_meta:
        raw_api = model_meta.get("api")
        if raw_api is None:
            return True, {}
        if not isinstance(raw_api, dict):
            raise ApiMetadataError("meta.api must be an object")
        return True, raw_api

    return False, {}


def normalize_api_parameters(
    model_name: str,
    columns: Dict[str, str],
    raw_parameters: Any,
    source: str,
) -> List[ApiParamSpec]:
    if raw_parameters is None:
        return []
    if not isinstance(raw_parameters, list):
        raise ApiMetadataError(f"{model_name}: api.parameters must be a list")

    normalized: List[ApiParamSpec] = []
    seen_names = set()
    column_names = set(columns.keys())

    for index, raw_param in enumerate(raw_parameters):
        if not isinstance(raw_param, dict):
            raise ApiMetadataError(f"{model_name}: api.parameters[{index}] must be an object")

        name = raw_param.get("name")
        column = raw_param.get("column")
        operator = raw_param.get("operator", "=")
        param_type = raw_param.get("type", "string")
        description = raw_param.get("description")
        case = raw_param.get("case")
        max_items = raw_param.get("max_items")

        if not isinstance(name, str) or not name.strip():
            raise ApiMetadataError(f"{model_name}: api.parameters[{index}] missing valid 'name'")
        name = name.strip()
        if name in seen_names:
            raise ApiMetadataError(f"{model_name}: duplicate parameter name '{name}'")
        seen_names.add(name)

        if not isinstance(column, str) or not column.strip():
            raise ApiMetadataError(f"{model_name}: parameter '{name}' missing valid 'column'")
        column = column.strip()
        if column not in column_names:
            raise ApiMetadataError(
                f"{model_name}: parameter '{name}' references unknown column '{column}'"
            )

        if not isinstance(operator, str):
            raise ApiMetadataError(f"{model_name}: parameter '{name}' has invalid operator")
        operator = operator.strip().upper()
        if operator not in ALLOWED_OPERATORS:
            raise ApiMetadataError(
                f"{model_name}: parameter '{name}' uses unsupported operator '{operator}'"
            )

        if not isinstance(param_type, str):
            raise ApiMetadataError(f"{model_name}: parameter '{name}' has invalid type")
        param_type = param_type.strip().lower()
        if param_type not in ALLOWED_TYPES:
            raise ApiMetadataError(
                f"{model_name}: parameter '{name}' uses unsupported type '{param_type}'"
            )

        if description is not None and not isinstance(description, str):
            raise ApiMetadataError(
                f"{model_name}: parameter '{name}' description must be a string"
            )

        if case is not None:
            if not isinstance(case, str):
                raise ApiMetadataError(f"{model_name}: parameter '{name}' has invalid case mode")
            case = case.strip().lower()
            if case not in ALLOWED_CASE_MODES:
                raise ApiMetadataError(
                    f"{model_name}: parameter '{name}' uses unsupported case mode '{case}'"
                )
            if param_type not in {"string", "string_list"}:
                raise ApiMetadataError(
                    f"{model_name}: parameter '{name}' case mode is only valid for string filters"
                )

        if operator == "IN" and param_type != "string_list":
            raise ApiMetadataError(
                f"{model_name}: parameter '{name}' can only use IN with type 'string_list'"
            )

        if max_items is not None:
            if param_type != "string_list":
                raise ApiMetadataError(
                    f"{model_name}: parameter '{name}' max_items is only valid for string_list"
                )
            if not isinstance(max_items, int) or isinstance(max_items, bool) or max_items <= 0:
                raise ApiMetadataError(
                    f"{model_name}: parameter '{name}' max_items must be a positive integer"
                )

        normalized.append(
            ApiParamSpec(
                name=name,
                column=column,
                operator=operator,
                type=param_type,
                description=description,
                case=case,
                max_items=max_items,
                source=source,
            )
        )

    return normalized


def _normalize_pagination(model_name: str, raw_pagination: Any) -> ApiPaginationSpec:
    if raw_pagination is None:
        return ApiPaginationSpec(enabled=False)
    if not isinstance(raw_pagination, dict):
        raise ApiMetadataError(f"{model_name}: api.pagination must be an object")

    enabled = raw_pagination.get("enabled", False)
    if not isinstance(enabled, bool):
        raise ApiMetadataError(f"{model_name}: api.pagination.enabled must be a boolean")

    default_limit = raw_pagination.get("default_limit")
    max_limit = raw_pagination.get("max_limit")

    if enabled:
        if (
            not isinstance(default_limit, int)
            or isinstance(default_limit, bool)
            or default_limit <= 0
        ):
            raise ApiMetadataError(
                f"{model_name}: api.pagination.default_limit must be a positive integer when pagination is enabled"
            )
        if not isinstance(max_limit, int) or isinstance(max_limit, bool) or max_limit <= 0:
            raise ApiMetadataError(
                f"{model_name}: api.pagination.max_limit must be a positive integer when pagination is enabled"
            )
        if default_limit > max_limit:
            raise ApiMetadataError(
                f"{model_name}: api.pagination.default_limit must be <= api.pagination.max_limit"
            )
    else:
        default_limit = None
        max_limit = None

    return ApiPaginationSpec(
        enabled=enabled,
        default_limit=default_limit,
        max_limit=max_limit,
    )


def _normalize_sort(
    model_name: str,
    columns: Dict[str, str],
    raw_sort: Any,
) -> List[ApiSortSpec]:
    if raw_sort is None:
        return []
    if not isinstance(raw_sort, list):
        raise ApiMetadataError(f"{model_name}: api.sort must be a list")

    normalized: List[ApiSortSpec] = []
    column_names = set(columns.keys())
    for index, raw_item in enumerate(raw_sort):
        if not isinstance(raw_item, dict):
            raise ApiMetadataError(f"{model_name}: api.sort[{index}] must be an object")

        column = raw_item.get("column")
        direction = raw_item.get("direction")
        if not isinstance(column, str) or not column.strip():
            raise ApiMetadataError(f"{model_name}: api.sort[{index}] missing valid 'column'")
        column = column.strip()
        if column not in column_names:
            raise ApiMetadataError(f"{model_name}: api.sort[{index}] uses unknown column '{column}'")

        if not isinstance(direction, str):
            raise ApiMetadataError(f"{model_name}: api.sort[{index}] missing valid 'direction'")
        direction = direction.strip().upper()
        if direction not in ALLOWED_SORT_DIRECTIONS:
            raise ApiMetadataError(
                f"{model_name}: api.sort[{index}] direction must be one of ASC or DESC"
            )

        normalized.append(ApiSortSpec(column=column, direction=direction))

    return normalized


def build_api_behavior(
    model_name: str,
    columns: Dict[str, str],
    raw_api_exists: bool,
    raw_api: Dict[str, Any],
) -> ApiBehaviorConfig:
    if not raw_api_exists:
        return ApiBehaviorConfig(
            metadata_enabled=False,
            methods=["GET"],
            allow_unfiltered=True,
            require_any_of=[],
            parameters=[],
            pagination=ApiPaginationSpec(enabled=False),
            sort=[],
        )

    methods_raw = raw_api.get("methods", ["GET"])
    if not isinstance(methods_raw, list) or not methods_raw:
        raise ApiMetadataError(f"{model_name}: api.methods must be a non-empty list")

    methods: List[str] = []
    seen_methods = set()
    for raw_method in methods_raw:
        if not isinstance(raw_method, str):
            raise ApiMetadataError(f"{model_name}: api.methods entries must be strings")
        method = raw_method.strip().upper()
        if method not in ALLOWED_METHODS:
            raise ApiMetadataError(f"{model_name}: unsupported method '{method}'")
        if method not in seen_methods:
            methods.append(method)
            seen_methods.add(method)

    allow_unfiltered = raw_api.get("allow_unfiltered", False)
    if not isinstance(allow_unfiltered, bool):
        raise ApiMetadataError(f"{model_name}: api.allow_unfiltered must be a boolean")

    require_any_of_raw = raw_api.get("require_any_of", [])
    if not isinstance(require_any_of_raw, list):
        raise ApiMetadataError(f"{model_name}: api.require_any_of must be a list")
    require_any_of: List[str] = []
    for name in require_any_of_raw:
        if not isinstance(name, str) or not name.strip():
            raise ApiMetadataError(f"{model_name}: api.require_any_of must contain non-empty strings")
        require_any_of.append(name.strip())

    parameters = normalize_api_parameters(
        model_name,
        columns,
        raw_api.get("parameters", []),
        source="metadata",
    )

    param_names = {param.name for param in parameters}
    missing_required = [name for name in require_any_of if name not in param_names]
    if missing_required:
        joined = ", ".join(missing_required)
        raise ApiMetadataError(
            f"{model_name}: api.require_any_of references undeclared parameters: {joined}"
        )
    if not allow_unfiltered and not parameters:
        raise ApiMetadataError(
            f"{model_name}: api.allow_unfiltered=false requires at least one declared parameter"
        )

    pagination = _normalize_pagination(model_name, raw_api.get("pagination"))
    sort = _normalize_sort(model_name, columns, raw_api.get("sort", []))

    return ApiBehaviorConfig(
        metadata_enabled=True,
        methods=methods,
        allow_unfiltered=allow_unfiltered,
        require_any_of=require_any_of,
        parameters=parameters,
        pagination=pagination,
        sort=sort,
    )
