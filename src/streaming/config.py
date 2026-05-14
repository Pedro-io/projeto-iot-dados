"""Configuração do consumer Bronze — lê do .env

Mapeia as variáveis de ambiente para as constantes usadas no código.
Nenhum valor hardcoded — tudo vem do .env
"""
import os
import sys


def _get_required_env(var_name: str) -> str:
    """Lê variável obrigatória — falha se não existir."""
    value = os.getenv(var_name)
    if value is None:
        print(f"ERRO: Variável '{var_name}' não definida no .env", file=sys.stderr)
        print(f"Verifique se o arquivo .env existe e contém essa variável.", file=sys.stderr)
        sys.exit(1)
    return value


def _get_env(var_name: str, fallback: str) -> str:
    """Lê variável opcional com fallback."""
    return os.getenv(var_name, fallback)


def _get_int_env(var_name: str, fallback: int) -> int:
    """Lê variável numérica."""
    value = os.getenv(var_name)
    if value is None:
        return fallback
    try:
        return int(value)
    except ValueError:
        print(f"AVISO: '{var_name}={value}' não é número. Usando {fallback}", file=sys.stderr)
        return fallback


DEFAULT_BOOTSTRAP_SERVERS = _get_required_env("KAFKA_BOOTSTRAP_SERVERS")
DEFAULT_TOPIC             = _get_required_env("KAFKA_TOPIC")
DEFAULT_GROUP_ID          = _get_required_env("KAFKA_GROUP_ID")


DEFAULT_MINIO_ENDPOINT   = _get_required_env("MINIO_ENDPOINT")
DEFAULT_MINIO_ACCESS_KEY = _get_required_env("MINIO_ACCESS_KEY")
DEFAULT_MINIO_SECRET_KEY = _get_required_env("MINIO_SECRET_KEY")
DEFAULT_BUCKET           = _get_required_env("DEFAULT_BUCKET")


DEFAULT_SCHEMA_REGISTRY_URL = _get_required_env("DEFAULT_SCHEMA_REGISTRY_URL")


DEFAULT_FLUSH_SIZE     = _get_int_env("DEFAULT_FLUSH_SIZE", 100)
DEFAULT_FLUSH_INTERVAL = _get_int_env("DEFAULT_FLUSH_INTERVAL", 30)