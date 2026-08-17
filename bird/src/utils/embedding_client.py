"""Embedding client over an OpenAI-compatible HTTP endpoint."""

from __future__ import annotations

from typing import Iterable, List

import requests
from loguru import logger


class EmbeddingClient:
    """Client for OpenAI-compatible embedding endpoints."""

    def __init__(
        self,
        model: str,
        endpoint: str,
        *,
        api_key: str = "",
        timeout: int = 120,
        dimensions: int = 1024,
        max_length: int = 32000,
    ) -> None:
        self.model = model
        self.endpoint = endpoint
        self.api_key = api_key
        self.timeout = timeout
        self.dimensions = dimensions
        self.max_length = max_length

    def encode(self, texts: Iterable[str]) -> List[List[float]]:
        """Encode a batch of texts into embedding vectors."""

        payload_texts = [(text or "")[: self.max_length] for text in texts]
        if not payload_texts:
            return []

        payload = {
            "input": payload_texts,
            "model": self.model,
            "encoding_format": "float",
            "dimensions": self.dimensions,
        }

        logger.debug(
            "Requesting embeddings",
            model=self.model,
            count=len(payload_texts),
        )
        headers = {}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        response = requests.post(
            url=self.endpoint,
            json=payload,
            headers=headers,
            timeout=self.timeout,
        )
        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            logger.error(
                f"Embedding request failed status={response.status_code} body={response.text[:1000]}",
            )
            raise exc
        data = response.json()
        return [item["embedding"] for item in data.get("data", [])]
