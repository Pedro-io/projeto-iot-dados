"""Módulo de configuração do logger.

Fornece uma instância compartilhada de `logger` (do Loguru) para ser importada
em diferentes tarefas, garantindo registro consistente. Sinks adicionais ou
formatação podem ser configurados centralmente aqui no futuro, se necessário.
"""

from loguru import logger

__all__ = ["logger"]