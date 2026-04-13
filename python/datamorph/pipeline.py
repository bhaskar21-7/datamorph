"""
Pipeline: chainable, composable data transformation engine.

Usage:
    result = (
        Pipeline(data)
        .apply(flatten)
        .apply(drop_nulls)
        .apply(rename_keys, mapping={"old": "new"})
        .apply(cast_types, schema={"age": int})
        .result()
    )
"""

from typing import Any, Callable, List, Tuple, Optional
import copy
import time


class PipelineError(Exception):
    """Raised when a pipeline step fails."""
    def __init__(self, message: str, step: int, transform: str):
        self.step = step
        self.transform = transform
        super().__init__(f"[Step {step} | {transform}] {message}")


class StepResult:
    def __init__(self, name: str, duration_ms: float, input_size: int, output_size: int):
        self.name = name
        self.duration_ms = duration_ms
        self.input_size = input_size
        self.output_size = output_size

    def __repr__(self):
        return (
            f"StepResult(name={self.name!r}, "
            f"duration_ms={self.duration_ms:.2f}, "
            f"input_size={self.input_size}, "
            f"output_size={self.output_size})"
        )


class Pipeline:
    """
    Composable, chainable data transformation pipeline.

    Supports lists of dicts (records) or single dicts.
    Tracks per-step timing and size metrics.
    """

    def __init__(self, data: Any, strict: bool = True):
        """
        Args:
            data: Input data — list of dicts or a single dict.
            strict: If True (default), raises PipelineError on failure.
                    If False, skips failed steps and logs warnings.
        """
        self._data = copy.deepcopy(data)
        self._strict = strict
        self._steps: List[Tuple[str, Callable, dict]] = []
        self._history: List[StepResult] = []
        self._warnings: List[str] = []

    def apply(self, transform: Callable, **kwargs) -> "Pipeline":
        """
        Add a transform step to the pipeline.

        Args:
            transform: A callable that accepts data as first arg.
            **kwargs: Additional keyword args passed to the transform.

        Returns:
            self (for chaining)
        """
        self._steps.append((transform.__name__, transform, kwargs))
        return self

    def result(self) -> Any:
        """
        Execute the pipeline and return the final transformed data.
        """
        data = self._data
        for i, (name, fn, kwargs) in enumerate(self._steps, start=1):
            input_size = len(data) if isinstance(data, list) else 1
            start = time.perf_counter()
            try:
                data = fn(data, **kwargs) if kwargs else fn(data)
            except Exception as e:
                if self._strict:
                    raise PipelineError(str(e), step=i, transform=name) from e
                else:
                    self._warnings.append(f"[Step {i} | {name}] Skipped due to error: {e}")
                    continue
            elapsed = (time.perf_counter() - start) * 1000
            output_size = len(data) if isinstance(data, list) else 1
            self._history.append(StepResult(name, elapsed, input_size, output_size))
        return data

    def stats(self) -> List[StepResult]:
        """Return per-step execution stats (available after .result() is called)."""
        return self._history

    def warnings(self) -> List[str]:
        """Return warnings from skipped steps (only if strict=False)."""
        return self._warnings

    def __repr__(self):
        steps = " -> ".join(name for name, _, _ in self._steps)
        return f"Pipeline([{steps}], strict={self._strict})"
