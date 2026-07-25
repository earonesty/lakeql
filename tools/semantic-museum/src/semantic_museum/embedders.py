from __future__ import annotations

import hashlib
from collections.abc import Sequence
from copy import deepcopy
from pathlib import Path
from typing import Protocol

import numpy as np
from PIL import Image


class ImageEmbedder(Protocol):
    model_id: str
    model_fingerprint: str
    preprocessing_id: str
    dimension: int
    device_name: str

    def encode(self, images: Sequence[Image.Image]) -> np.ndarray: ...


class DeterministicImageEmbedder:
    model_id = "deterministic-test-embedder"
    model_fingerprint = hashlib.sha256(model_id.encode()).hexdigest()
    preprocessing_id = "rgb-bytes:sha256:normal-512"
    dimension = 512
    device_name = "cpu:test"

    def encode(self, images: Sequence[Image.Image]) -> np.ndarray:
        vectors: list[np.ndarray] = []
        for image in images:
            rgb = image.convert("RGB")
            digest = hashlib.sha256(
                rgb.width.to_bytes(4, "big")
                + rgb.height.to_bytes(4, "big")
                + rgb.tobytes()
            ).digest()
            seed = int.from_bytes(digest[:8], "big")
            generator = np.random.default_rng(seed)
            vector = generator.standard_normal(self.dimension, dtype=np.float32)
            vector /= np.linalg.norm(vector)
            vectors.append(vector)
        return np.stack(vectors) if vectors else np.empty((0, self.dimension), dtype=np.float32)


class MobileClip2S0Embedder:
    model_id = "MobileCLIP2-S0:apple/MobileCLIP2-S0:mobileclip2_s0.pt"
    preprocessing_id = (
        "open_clip-3.3.0:MobileCLIP2-S0:"
        "resize-shortest-bicubic:center-crop-256:rgb:mean-0:std-1"
    )
    dimension = 512

    def __init__(self, *, device: str = "auto", checkpoint: Path | None = None) -> None:
        try:
            import torch
            from huggingface_hub import hf_hub_download
            from open_clip import create_model_and_transforms
        except ImportError as error:
            raise RuntimeError(
                "MobileCLIP2 requires the harness ml extra: uv sync --extra ml"
            ) from error

        selected = "cuda" if device == "auto" and torch.cuda.is_available() else device
        if selected == "auto":
            selected = "cpu"
        if selected == "cuda" and not torch.cuda.is_available():
            raise RuntimeError("CUDA was requested but torch.cuda.is_available() is false")
        checkpoint_path = checkpoint or Path(
            hf_hub_download(repo_id="apple/MobileCLIP2-S0", filename="mobileclip2_s0.pt")
        )
        self.model_fingerprint = _sha256_file(checkpoint_path)
        model, _, preprocess = create_model_and_transforms(
            "MobileCLIP2-S0",
            pretrained=str(checkpoint_path),
            image_mean=(0.0, 0.0, 0.0),
            image_std=(1.0, 1.0, 1.0),
        )
        model.eval()
        model = _reparameterize_mobileclip(model)
        model.to(selected)
        self._torch = torch
        self._model = model
        self._preprocess = preprocess
        self._device = selected
        self.device_name = (
            f"cuda:{torch.cuda.get_device_name(0)}" if selected == "cuda" else selected
        )

    def encode(self, images: Sequence[Image.Image]) -> np.ndarray:
        if not images:
            return np.empty((0, self.dimension), dtype=np.float32)
        torch = self._torch
        batch = torch.stack([self._preprocess(image.convert("RGB")) for image in images]).to(
            self._device,
            non_blocking=self._device == "cuda",
        )
        with torch.inference_mode():
            if self._device == "cuda":
                with torch.autocast(device_type="cuda", dtype=torch.float16):
                    features = self._model.encode_image(batch)
            else:
                features = self._model.encode_image(batch)
            features = torch.nn.functional.normalize(features.float(), dim=-1)
        values = features.cpu().numpy().astype(np.float32, copy=False)
        if values.shape != (len(images), self.dimension):
            expected_shape = (len(images), self.dimension)
            raise RuntimeError(
                f"MobileCLIP2 returned shape {values.shape}, expected {expected_shape}"
            )
        if not np.isfinite(values).all():
            raise RuntimeError("MobileCLIP2 returned non-finite embeddings")
        return values


def _reparameterize_mobileclip(model: object) -> object:
    copied = deepcopy(model)
    modules = list(copied.modules())  # type: ignore[attr-defined]
    reparameterized = 0
    for module in modules:
        operation = getattr(module, "reparameterize", None)
        if callable(operation):
            operation()
            reparameterized += 1
    if reparameterized == 0:
        raise RuntimeError("MobileCLIP model exposed no reparameterizable modules")
    return copied


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()
