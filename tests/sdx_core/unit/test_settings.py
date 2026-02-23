from __future__ import annotations

from typing import Any, cast

import pytest
from pydantic import ValidationError

from sdx_core.settings import CoreSettings


def _build_settings(**overrides: object) -> CoreSettings:
    values: dict[str, object] = {
        "kafka_bootstrap_servers": "localhost:9092",
        "kafka_group_id": "qali-geocode",
        "kafka_input_topic": "in",
        "kafka_auto_offset_reset": "earliest",
    }
    values.update(overrides)
    return CoreSettings(**cast(Any, values))


def test_core_settings_builds_kafka_common_config() -> None:
    settings = _build_settings()
    config = settings.kafka_common_config()

    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["security.protocol"] == "plaintext"
    assert config["reconnect.backoff.ms"] == 100
    assert config["reconnect.backoff.max.ms"] == 10_000
    assert config["socket.connection.setup.timeout.ms"] == 30_000
    assert config["broker.address.family"] == "any"
    assert "client.id" not in config
    assert "sasl.username" not in config


def test_core_settings_normalizes_sasl_ssl_and_defaults_mechanism() -> None:
    settings = _build_settings(
        kafka_security_protocol="SASL_SSL",
        kafka_sasl_username="api-key",
        kafka_sasl_password="api-secret",
    )
    config = settings.kafka_common_config()

    assert settings.kafka_security_protocol == "sasl_ssl"
    assert config["security.protocol"] == "sasl_ssl"
    assert config["sasl.username"] == "api-key"
    assert config["sasl.password"] == "api-secret"
    assert config["sasl.mechanisms"] == "PLAIN"


def test_core_settings_includes_optional_client_and_ssl_ca() -> None:
    settings = _build_settings(
        kafka_client_id="qali-client",
        kafka_ssl_ca_location="/etc/ssl/certs/ca.pem",
    )
    config = settings.kafka_common_config()

    assert config["client.id"] == "qali-client"
    assert config["ssl.ca.location"] == "/etc/ssl/certs/ca.pem"


def test_core_settings_rejects_sasl_ssl_without_username() -> None:
    with pytest.raises(ValidationError):
        _build_settings(
            kafka_security_protocol="sasl_ssl",
            kafka_sasl_password="api-secret",
        )


def test_core_settings_rejects_sasl_ssl_without_password() -> None:
    with pytest.raises(ValidationError):
        _build_settings(
            kafka_security_protocol="sasl_ssl",
            kafka_sasl_username="api-key",
        )


def test_core_settings_rejects_sasl_fields_for_plaintext() -> None:
    with pytest.raises(ValidationError):
        _build_settings(
            kafka_security_protocol="plaintext",
            kafka_sasl_username="api-key",
            kafka_sasl_password="api-secret",
        )


def test_core_settings_rejects_invalid_reconnect_backoff_bounds() -> None:
    with pytest.raises(ValidationError):
        _build_settings(
            kafka_reconnect_backoff_ms=5000,
            kafka_reconnect_backoff_max_ms=1000,
        )


def test_core_settings_rejects_non_positive_socket_setup_timeout() -> None:
    with pytest.raises(ValidationError):
        _build_settings(kafka_socket_connection_setup_timeout_ms=0)
