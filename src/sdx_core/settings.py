from __future__ import annotations

from typing import Literal

from pydantic import ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

KafkaSecurityProtocol = Literal["plaintext", "ssl", "sasl_ssl", "sasl_plaintext"]


def prefixed_settings_config(prefix: str) -> SettingsConfigDict:
    """Build standard Pydantic settings config for prefixed environments."""
    return SettingsConfigDict(env_prefix=prefix, case_sensitive=False)


class CoreSettings(BaseSettings):
    """Shared settings for Kafka/FastStream microservice runtimes."""

    model_config = SettingsConfigDict(case_sensitive=False)

    kafka_bootstrap_servers: str
    kafka_group_id: str
    kafka_input_topic: str
    kafka_security_protocol: KafkaSecurityProtocol = "plaintext"
    kafka_sasl_mechanism: str | None = None
    kafka_sasl_username: str | None = None
    kafka_sasl_password: str | None = None
    kafka_ssl_ca_location: str | None = None
    kafka_client_id: str | None = None
    kafka_reconnect_backoff_ms: int = 100
    kafka_reconnect_backoff_max_ms: int = 10_000
    kafka_socket_connection_setup_timeout_ms: int = 30_000
    kafka_broker_address_family: Literal["any", "v4", "v6"] = "any"
    kafka_auto_offset_reset: Literal["earliest", "latest", "error"]

    @field_validator("kafka_security_protocol", mode="before")
    @classmethod
    def _normalize_kafka_security_protocol(cls, value: object) -> object:
        if isinstance(value, str):
            return value.lower()
        return value

    @field_validator(
        "kafka_bootstrap_servers",
        "kafka_group_id",
        "kafka_input_topic",
        mode="before",
    )
    @classmethod
    def _validate_required_kafka_string(
        cls, value: object, info: ValidationInfo
    ) -> object:
        if not isinstance(value, str):
            return value
        normalized = value.strip()
        if not normalized:
            raise ValueError(f"{info.field_name} must be non-empty")
        return normalized

    @model_validator(mode="after")
    def _validate_core_settings(self) -> CoreSettings:
        if self.kafka_reconnect_backoff_ms < 0:
            raise ValueError("kafka_reconnect_backoff_ms must be >= 0")
        if self.kafka_reconnect_backoff_max_ms < self.kafka_reconnect_backoff_ms:
            raise ValueError(
                "kafka_reconnect_backoff_max_ms must be >= kafka_reconnect_backoff_ms"
            )
        if self.kafka_socket_connection_setup_timeout_ms <= 0:
            raise ValueError("kafka_socket_connection_setup_timeout_ms must be > 0")

        protocol = self.kafka_security_protocol
        sasl_values = (
            self.kafka_sasl_username,
            self.kafka_sasl_password,
            self.kafka_sasl_mechanism,
        )
        sasl_set = any(value not in (None, "") for value in sasl_values)

        if protocol in {"sasl_ssl", "sasl_plaintext"}:
            if not self.kafka_sasl_username:
                raise ValueError(
                    "kafka_sasl_username is required when kafka_security_protocol is "
                    "sasl_*"
                )
            if not self.kafka_sasl_password:
                raise ValueError(
                    "kafka_sasl_password is required when kafka_security_protocol is "
                    "sasl_*"
                )
        elif sasl_set:
            raise ValueError(
                "kafka_sasl_* settings require kafka_security_protocol sasl_ssl or "
                "sasl_plaintext"
            )
        return self

    def kafka_common_config(self) -> dict[str, str | int | float | bool | None]:
        """Build shared Kafka client configuration dictionary."""
        config: dict[str, str | int | float | bool | None] = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "security.protocol": self.kafka_security_protocol,
            "reconnect.backoff.ms": self.kafka_reconnect_backoff_ms,
            "reconnect.backoff.max.ms": self.kafka_reconnect_backoff_max_ms,
            "socket.connection.setup.timeout.ms": (
                self.kafka_socket_connection_setup_timeout_ms
            ),
            "broker.address.family": self.kafka_broker_address_family,
        }
        if self.kafka_client_id:
            config["client.id"] = self.kafka_client_id
        if self.kafka_ssl_ca_location:
            config["ssl.ca.location"] = self.kafka_ssl_ca_location

        protocol = self.kafka_security_protocol
        if protocol in {"sasl_ssl", "sasl_plaintext"}:
            config["sasl.mechanisms"] = self.kafka_sasl_mechanism or "PLAIN"
            config["sasl.username"] = self.kafka_sasl_username
            config["sasl.password"] = self.kafka_sasl_password

        return config
