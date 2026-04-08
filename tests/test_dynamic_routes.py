from copy import deepcopy
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api_metadata import ApiMetadataError, build_api_behavior
from app.config import settings
from app.factory import build_router
from app.security import limiter
from tests.conftest import TEST_API_KEYS


VALIDATOR_COLUMNS = {
    "slot": "UInt64",
    "validator_index": "UInt32",
    "balance": "UInt64",
    "status": "String",
    "pubkey": "String",
    "withdrawal_credentials": "String",
    "effective_balance": "UInt64",
    "slashed": "UInt8",
    "activation_eligibility_epoch": "UInt64",
    "activation_epoch": "UInt64",
    "exit_epoch": "UInt64",
    "withdrawable_epoch": "UInt64",
    "slot_timestamp": "DateTime64(0, 'UTC')",
}

VALIDATOR_API = {
    "methods": ["GET", "POST"],
    "allow_unfiltered": False,
    "require_any_of": ["withdrawal_credentials", "pubkey"],
    "parameters": [
        {
            "name": "withdrawal_credentials",
            "column": "withdrawal_credentials",
            "operator": "IN",
            "type": "string_list",
            "case": "lower",
            "max_items": 200,
            "description": "Withdrawal credential value(s)",
        },
        {
            "name": "pubkey",
            "column": "pubkey",
            "operator": "IN",
            "type": "string_list",
            "case": "lower",
            "max_items": 200,
            "description": "Validator public key(s)",
        },
    ],
    "pagination": {
        "enabled": True,
        "default_limit": 100,
        "max_limit": 5000,
    },
    "sort": [
        {"column": "validator_index", "direction": "ASC"},
    ],
}


def _make_rows(count: int) -> list[dict]:
    return [
        {
            "slot": 27322611,
            "validator_index": index,
            "balance": 0,
            "status": "withdrawal_done",
            "pubkey": f"0xpubkey{index:02d}",
            "withdrawal_credentials": "0xabc",
            "effective_balance": 0,
            "slashed": 0,
            "activation_eligibility_epoch": 0,
            "activation_epoch": 0,
            "exit_epoch": 1156542,
            "withdrawable_epoch": 1156798,
            "slot_timestamp": "2026-04-07T23:59:55",
        }
        for index in range(count)
    ]


def _make_model_entry(
    model_name: str,
    path_tags: list[str],
    raw_api: dict,
) -> dict:
    return {
        "node": {
            "name": model_name,
            "description": f"{model_name} description",
            "config": {"meta": {"api": raw_api}},
            "meta": {"api": raw_api},
        },
        "tags": path_tags,
        "columns": deepcopy(VALIDATOR_COLUMNS),
        "table_name": f"dbt.{model_name}",
    }


def _build_manifest_mock(models: dict[str, dict]) -> MagicMock:
    mock_manifest = MagicMock()
    mock_manifest.get_all_models.return_value = list(models.keys())
    mock_manifest.get_model.side_effect = lambda model_name: models[model_name]["node"]
    mock_manifest.get_tags.side_effect = lambda model_name: models[model_name]["tags"]
    mock_manifest.get_columns.side_effect = lambda model_name: models[model_name]["columns"]
    mock_manifest.get_table_name.side_effect = lambda model_name: models[model_name]["table_name"]
    return mock_manifest


class DynamicRouteHarness:
    def __init__(self, models: dict[str, dict], query_rows: list[dict]):
        self.models = models
        self.query_rows = query_rows
        self._client: TestClient | None = None
        self.app: FastAPI | None = None
        self.mock_query = None
        self._patches = []
        self._original_api_keys = None

    def __enter__(self):
        self._original_api_keys = settings.API_KEYS
        settings.API_KEYS = TEST_API_KEYS.copy()

        manifest_patch = patch("app.factory.manifest", _build_manifest_mock(self.models))
        query_patch = patch("app.factory.ClickHouseClient.query")

        self._patches = [manifest_patch, query_patch]
        started = [patcher.start() for patcher in self._patches]
        self.mock_query = started[1]
        self.mock_query.return_value = self.query_rows

        self.app = FastAPI()
        self.app.state.limiter = limiter
        self.app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
        router, _specs, _warnings = build_router()
        self.app.include_router(router, prefix="/v1")
        self._client = TestClient(self.app, raise_server_exceptions=False)
        self._client.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        if self._client is not None:
            self._client.__exit__(exc_type, exc, tb)
        for patcher in reversed(self._patches):
            patcher.stop()
        settings.API_KEYS = self._original_api_keys

    @property
    def client(self) -> TestClient:
        assert self._client is not None
        return self._client


class TestPaginationMetadata:
    def test_exclude_from_api_defaults_to_false(self):
        behavior = build_api_behavior(
            "api_example",
            {"validator_index": "UInt32"},
            True,
            {
                "methods": ["GET"],
                "allow_unfiltered": True,
            },
        )

        assert behavior.exclude_from_api is False

    def test_exclude_from_api_accepts_true(self):
        behavior = build_api_behavior(
            "api_example",
            {"validator_index": "UInt32"},
            True,
            {
                "methods": ["GET"],
                "allow_unfiltered": True,
                "exclude_from_api": True,
            },
        )

        assert behavior.exclude_from_api is True

    def test_exclude_from_api_rejects_non_boolean(self):
        with pytest.raises(ApiMetadataError, match="api.exclude_from_api must be a boolean"):
            build_api_behavior(
                "api_example",
                {"validator_index": "UInt32"},
                True,
                {
                    "methods": ["GET"],
                    "allow_unfiltered": True,
                    "exclude_from_api": "yes",
                },
            )

    def test_pagination_response_defaults_to_list(self):
        behavior = build_api_behavior(
            "api_example",
            {"validator_index": "UInt32"},
            True,
            {
                "methods": ["GET"],
                "allow_unfiltered": True,
                "pagination": {
                    "enabled": True,
                    "default_limit": 10,
                    "max_limit": 100,
                },
            },
        )

        assert behavior.pagination.response_mode == "list"

    def test_pagination_response_accepts_envelope(self):
        behavior = build_api_behavior(
            "api_example",
            {"validator_index": "UInt32"},
            True,
            {
                "methods": ["GET"],
                "allow_unfiltered": True,
                "pagination": {
                    "enabled": True,
                    "default_limit": 10,
                    "max_limit": 100,
                    "response": "envelope",
                },
            },
        )

        assert behavior.pagination.response_mode == "envelope"

    def test_pagination_response_rejects_unknown_value(self):
        with pytest.raises(ApiMetadataError, match="api.pagination.response must be one of"):
            build_api_behavior(
                "api_example",
                {"validator_index": "UInt32"},
                True,
                {
                    "methods": ["GET"],
                    "allow_unfiltered": True,
                    "pagination": {
                        "enabled": True,
                        "default_limit": 10,
                        "max_limit": 100,
                        "response": "headers",
                    },
                },
            )


class TestEnvelopePaginationRoutes:
    def _models(self) -> dict[str, dict]:
        envelope_api = deepcopy(VALIDATOR_API)
        envelope_api["pagination"]["response"] = "envelope"
        list_api = deepcopy(VALIDATOR_API)

        return {
            "api_consensus_validators_status_latest": _make_model_entry(
                "api_consensus_validators_status_latest",
                ["production", "consensus", "tier1", "api:validators_status", "granularity:latest"],
                envelope_api,
            ),
            "api_consensus_validators_status_daily": _make_model_entry(
                "api_consensus_validators_status_daily",
                ["production", "consensus", "tier1", "api:validators_status", "granularity:daily"],
                list_api,
            ),
        }

    def test_post_envelope_response_includes_pagination_metadata(self):
        with DynamicRouteHarness(self._models(), _make_rows(10)) as harness:
            response = harness.client.post(
                "/v1/consensus/validators_status/latest",
                headers={"X-API-Key": "test-key-tier1"},
                json={
                    "withdrawal_credentials": "0xabc",
                    "limit": 10,
                    "offset": 0,
                },
            )

        assert response.status_code == 200
        payload = response.json()
        assert sorted(payload.keys()) == ["items", "pagination"]
        assert len(payload["items"]) == 10
        assert payload["pagination"] == {
            "limit": 10,
            "offset": 0,
            "returned": 10,
            "has_more": False,
        }
        assert harness.mock_query.call_args.args[1]["limit"] == 11
        assert harness.mock_query.call_args.args[1]["offset"] == 0

    def test_post_envelope_response_trims_items_and_sets_has_more(self):
        with DynamicRouteHarness(self._models(), _make_rows(11)) as harness:
            response = harness.client.post(
                "/v1/consensus/validators_status/latest",
                headers={"X-API-Key": "test-key-tier1"},
                json={
                    "withdrawal_credentials": "0xabc",
                    "limit": 10,
                    "offset": 0,
                },
            )

        assert response.status_code == 200
        payload = response.json()
        assert len(payload["items"]) == 10
        assert payload["items"][-1]["validator_index"] == 9
        assert payload["pagination"]["returned"] == 10
        assert payload["pagination"]["has_more"] is True
        assert harness.mock_query.call_args.args[1]["limit"] == 11

    def test_get_envelope_response_uses_resolved_limit_and_offset(self):
        with DynamicRouteHarness(self._models(), _make_rows(2)) as harness:
            response = harness.client.get(
                "/v1/consensus/validators_status/latest",
                headers={"X-API-Key": "test-key-tier1"},
                params={
                    "withdrawal_credentials": "0xabc",
                    "limit": 2,
                    "offset": 3,
                },
            )

        assert response.status_code == 200
        payload = response.json()
        assert payload["pagination"] == {
            "limit": 2,
            "offset": 3,
            "returned": 2,
            "has_more": False,
        }
        assert harness.mock_query.call_args.args[1]["limit"] == 3
        assert harness.mock_query.call_args.args[1]["offset"] == 3

    def test_default_paginated_endpoint_still_returns_bare_array(self):
        with DynamicRouteHarness(self._models(), _make_rows(2)) as harness:
            response = harness.client.post(
                "/v1/consensus/validators_status/daily",
                headers={"X-API-Key": "test-key-tier1"},
                json={
                    "withdrawal_credentials": "0xabc",
                    "limit": 2,
                    "offset": 1,
                },
            )

        assert response.status_code == 200
        payload = response.json()
        assert isinstance(payload, list)
        assert len(payload) == 2
        assert harness.mock_query.call_args.args[1]["limit"] == 2
        assert harness.mock_query.call_args.args[1]["offset"] == 1

    def test_openapi_documents_envelope_response_shape(self):
        with DynamicRouteHarness(self._models(), _make_rows(1)) as harness:
            openapi = harness.app.openapi()

        get_schema = openapi["paths"]["/v1/consensus/validators_status/latest"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
        post_schema = openapi["paths"]["/v1/consensus/validators_status/latest"]["post"]["responses"]["200"]["content"]["application/json"]["schema"]

        for schema in (get_schema, post_schema):
            assert schema["type"] == "object"
            assert set(schema["properties"].keys()) == {"items", "pagination"}
            assert set(schema["properties"]["pagination"]["properties"].keys()) == {
                "limit",
                "offset",
                "returned",
                "has_more",
            }


class TestExcludedDynamicRoutes:
    def _models(self) -> dict[str, dict]:
        visible_api = deepcopy(VALIDATOR_API)
        hidden_api = deepcopy(VALIDATOR_API)
        hidden_api["exclude_from_api"] = True

        return {
            "api_consensus_visible_latest": _make_model_entry(
                "api_consensus_visible_latest",
                ["production", "consensus", "tier1", "api:visible_validators", "granularity:latest"],
                visible_api,
            ),
            "api_consensus_hidden_latest": _make_model_entry(
                "api_consensus_hidden_latest",
                ["production", "consensus", "tier1", "api:hidden_validators", "granularity:latest"],
                hidden_api,
            ),
        }

    def test_excluded_model_is_not_registered_and_returns_404(self):
        with DynamicRouteHarness(self._models(), _make_rows(1)) as harness:
            openapi = harness.app.openapi()
            hidden_response = harness.client.get(
                "/v1/consensus/hidden_validators/latest",
                headers={"X-API-Key": "test-key-tier1"},
            )
            visible_response = harness.client.get(
                "/v1/consensus/visible_validators/latest",
                headers={"X-API-Key": "test-key-tier1"},
                params={"withdrawal_credentials": "0xabc"},
            )

        assert "/v1/consensus/hidden_validators/latest" not in openapi["paths"]
        assert "/v1/consensus/visible_validators/latest" in openapi["paths"]
        assert hidden_response.status_code == 404
        assert visible_response.status_code == 200

    def test_excluded_model_is_skipped_on_rebuild_even_with_previous_specs(self):
        visible_api = deepcopy(VALIDATOR_API)
        excluded_api = deepcopy(VALIDATOR_API)
        excluded_api["exclude_from_api"] = True

        model_name = "api_consensus_hidden_latest"
        tags = ["production", "consensus", "tier1", "api:hidden_validators", "granularity:latest"]

        with patch("app.factory.manifest", _build_manifest_mock({
            model_name: _make_model_entry(model_name, tags, visible_api),
        })):
            _router, previous_specs, _warnings = build_router()

        assert model_name in previous_specs

        with patch("app.factory.manifest", _build_manifest_mock({
            model_name: _make_model_entry(model_name, tags, excluded_api),
        })):
            rebuilt_router, rebuilt_specs, rebuilt_warnings = build_router(previous_specs=previous_specs)

        route_paths = {route.path for route in rebuilt_router.routes}
        assert model_name not in rebuilt_specs
        assert "/consensus/hidden_validators/latest" not in route_paths
        assert any("excluded from API via meta.api.exclude_from_api" in warning for warning in rebuilt_warnings)
