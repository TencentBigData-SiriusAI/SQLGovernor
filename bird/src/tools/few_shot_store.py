"""Few-shot example loader and similarity search."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional

import faiss
import numpy as np
from loguru import logger
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from config import Settings
from ..utils.embedding_client import EmbeddingClient


@dataclass
class FewShotExample:
    """Lightweight container for few-shot data."""

    question: str
    evidence: str
    sql: str
    db_id: Optional[str]

    def render_prompt(self) -> str:
        sections = [f"Question: {self.question.strip()}" if self.question else "Question: (missing)"]
        if self.evidence.strip():
            sections.append(f"Evidence: {self.evidence.strip()}")
        sections.append("SQL:\n" + self.sql.strip())
        return "\n".join(sections)


def _compose_query_text(question: str, evidence: str) -> str:
    parts = [question or ""]
    if evidence:
        parts.append(evidence)
    return " \n ".join(part.strip() for part in parts if part).strip()


class _FewShotStore:
    def __init__(self, data_path: Path) -> None:
        self._data_path = data_path
        self._lock = Lock()
        self._ready = False
        self._vectorizer: Optional[TfidfVectorizer] = None
        self._matrix = None
        self._examples: List[FewShotExample] = []

    def _compose_text(self, item: Dict[str, str]) -> str:
        parts = [item.get("question", "")]
        evidence = item.get("evidence") or item.get("external_knowledge") or ""
        if evidence:
            parts.append(evidence)
        topic = item.get("topic")
        if topic:
            parts.append(topic)
        return " \n ".join(part.strip() for part in parts if part)

    def _ensure_ready(self) -> None:
        if self._ready:
            return
        with self._lock:
            if self._ready:
                return
            if not self._data_path.exists():
                logger.warning(f"Few-shot file missing: {self._data_path}")
                self._ready = True
                return
            try:
                payload = json.loads(self._data_path.read_text(encoding="utf-8"))
            except Exception as exc:  # pragma: no cover
                logger.warning("Unable to read few-shot file", error=str(exc))
                self._ready = True
                return

            documents: List[str] = []
            examples: List[FewShotExample] = []
            for item in payload:
                sql = item.get("SQL") or item.get("sql") or ""
                question = item.get("question", "").strip()
                if not sql or not question:
                    continue
                evidence = (item.get("evidence") or "").strip()
                examples.append(
                    FewShotExample(
                        question=question,
                        evidence=evidence,
                        sql=sql.strip(),
                        db_id=item.get("db_id"),
                    )
                )
                documents.append(self._compose_text(item))

            if not examples:
                logger.warning("Few-shot payload empty after filtering")
                self._ready = True
                return

            vectorizer = TfidfVectorizer(max_features=4096, ngram_range=(1, 2))
            matrix = vectorizer.fit_transform(documents)

            self._examples = examples
            self._vectorizer = vectorizer
            self._matrix = matrix
            self._ready = True
            logger.info(
                "Few-shot store ready",
                sample_count=len(examples),
                path=str(self._data_path),
            )

    def query(self, question: str, evidence: str, top_k: int, min_score: float) -> List[FewShotExample]:
        self._ensure_ready()
        if not self._examples or not self._vectorizer or self._matrix is None:
            return []

        query_text = _compose_query_text(question, evidence)
        if not query_text:
            # Return the first top_k examples as-is.
            return self._examples[:top_k]

        query_vec = self._vectorizer.transform([query_text])
        scores = cosine_similarity(query_vec, self._matrix)[0]
        ranked = sorted(
            enumerate(scores),
            key=lambda item: item[1],
            reverse=True,
        )

        selected: List[FewShotExample] = []
        for idx, score in ranked:
            if len(selected) >= top_k:
                break
            if score < min_score:
                break
            selected.append(self._examples[idx])

        if len(selected) < top_k:
            # Fill remaining slots if fewer than top_k matched.
            needed = top_k - len(selected)
            for idx, score in ranked:
                candidate = self._examples[idx]
                if candidate in selected:
                    continue
                selected.append(candidate)
                if len(selected) >= top_k:
                    break

        return selected[:top_k]


class _EmbeddingFewShotStore:
    def __init__(self, data_path: Path, embedding_path: Path) -> None:
        self._data_path = data_path
        self._embedding_path = embedding_path
        self._lock = Lock()
        self._ready = False
        self._examples: List[FewShotExample] = []
        self._index: Optional[faiss.IndexFlatL2] = None
        self._embeddings: Optional[np.ndarray] = None
        self._client = EmbeddingClient(
            model=Settings.FEW_SHOT_EMBEDDING_MODEL,
            endpoint=Settings.FEW_SHOT_EMBEDDING_ENDPOINT,
            api_key=Settings.FEW_SHOT_EMBEDDING_API_KEY,
            timeout=Settings.FEW_SHOT_EMBEDDING_TIMEOUT,
            dimensions=Settings.FEW_SHOT_EMBEDDING_DIM,
            max_length=Settings.FEW_SHOT_MAX_TEXT_LENGTH,
        )

    def _ensure_ready(self) -> None:
        if self._ready:
            return
        with self._lock:
            if self._ready:
                return
            if not self._data_path.exists() or not self._embedding_path.exists():
                logger.warning(
                    "Embedding few-shot artifacts missing",
                    data=str(self._data_path),
                    embeddings=str(self._embedding_path),
                )
                self._ready = True
                return

            try:
                payload = json.loads(self._data_path.read_text(encoding="utf-8"))
                embeddings = np.load(self._embedding_path)
            except Exception as exc:  # pragma: no cover - load failure
                logger.error("Unable to load embedding few-shot store", error=str(exc))
                self._ready = True
                return

            examples: List[FewShotExample] = []
            for item in payload:
                sql = item.get("sql") or item.get("SQL") or ""
                question = (item.get("question") or "").strip()
                if not sql or not question:
                    continue
                examples.append(
                    FewShotExample(
                        question=question,
                        evidence=(item.get("evidence") or "").strip(),
                        sql=sql.strip(),
                        db_id=item.get("db_id"),
                    )
                )

            if not examples:
                logger.warning("Embedding few-shot payload empty")
                self._ready = True
                return

            array = np.asarray(embeddings, dtype="float32")
            if array.shape[0] != len(examples):
                logger.error(
                    "Embedding count mismatch",
                    vector_count=array.shape[0],
                    example_count=len(examples),
                )
                self._ready = True
                return

            index = faiss.IndexFlatL2(array.shape[1])
            index.add(array)

            self._embeddings = array
            self._examples = examples
            self._index = index
            self._ready = True
            logger.info(
                "Embedding few-shot store ready",
                examples=len(examples),
                dim=array.shape[1],
            )

    def query(self, question: str, evidence: str, top_k: int, min_score: float) -> List[FewShotExample]:
        self._ensure_ready()
        if not self._examples or self._index is None:
            return []

        query_text = _compose_query_text(question, evidence)
        if not query_text:
            return self._examples[:top_k]

        try:
            embedding = self._client.encode([query_text])[0]
        except Exception as exc:  # pragma: no cover - embedding failure
            logger.error("Embedding query failed", error=str(exc))
            return []

        vector = np.asarray(embedding, dtype="float32").reshape(1, -1)
        if vector.shape[1] != self._embeddings.shape[1]:
            logger.error(
                "Embedding dimension mismatch",
                expected=self._embeddings.shape[1],
                received=vector.shape[1],
            )
            return []

        limit = min(max(top_k, 1), len(self._examples))
        distances, indices = self._index.search(vector, limit)
        ranked = list(zip(indices[0], distances[0]))

        def _to_similarity(distance: float) -> float:
            return 1.0 / (1.0 + distance)

        selected: List[FewShotExample] = []
        for idx, distance in ranked:
            similarity = _to_similarity(distance)
            if min_score > 0 and similarity < min_score:
                break
            selected.append(self._examples[idx])
            if len(selected) >= top_k:
                break

        if len(selected) < top_k:
            for idx, _ in ranked:
                candidate = self._examples[idx]
                if candidate in selected:
                    continue
                selected.append(candidate)
                if len(selected) >= top_k:
                    break

        return selected[:top_k]


_STORE: Optional[object] = None


def _get_store() -> Optional[_FewShotStore]:
    global _STORE
    if _STORE is not None:
        return _STORE

    if Settings.FEW_SHOT_MODE == "embedding":
        if Settings.FEW_SHOT_FILE is None or Settings.FEW_SHOT_EMBEDDINGS_FILE is None:
            logger.warning(
                "Embedding few-shot requires FEW_SHOT_FILE and FEW_SHOT_EMBEDDINGS_FILE",
            )
        else:
            _STORE = _EmbeddingFewShotStore(
                Settings.FEW_SHOT_FILE,
                Settings.FEW_SHOT_EMBEDDINGS_FILE,
            )
            return _STORE

    if Settings.FEW_SHOT_FILE is None:
        logger.info("Few-shot file not configured; skipping few-shot injection")
        return None

    _STORE = _FewShotStore(Settings.FEW_SHOT_FILE)
    return _STORE


def get_few_shot_examples(
    question: str,
    evidence: str,
    top_k: int,
    *,
    min_score: float | None = None,
) -> List[FewShotExample]:
    store = _get_store()
    if store is None or top_k <= 0:
        return []
    return store.query(
        question=question,
        evidence=evidence,
        top_k=top_k,
        min_score=
        min_score if min_score is not None else Settings.FEW_SHOT_MIN_SIMILARITY,
    )
