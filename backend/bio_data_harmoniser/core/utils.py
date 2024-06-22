import contextlib
import enum
import re
from dataclasses import dataclass, field
from typing import ClassVar, Optional

import pydantic
from airflow.task.task_runner import standard_task_runner
from loguru import logger


def to_snake_case(s: str) -> str:
    # Replace all non-word characters (everything except numbers and letters) with "_"
    s = re.sub(r"\W+", "_", s)

    # Replace all digit-word boundaries with "_"
    s = re.sub(r"(\d)([a-zA-Z])", r"\1_\2", s)
    s = re.sub(r"([a-zA-Z])(\d)", r"\1_\2", s)

    # Convert CamelCase to snake_case
    s = re.sub("([a-z0-9])([A-Z])", r"\1_\2", s)

    # Lowercase all characters in the string
    s = s.lower()

    # Remove leading/trailing underscores
    s = s.strip("_")

    return s


class Mapping(pydantic.BaseModel):
    id: str
    mention: str
    types: list[str]
    name: str
    score: float
    normalised_score: float | None = None
    mapping_id: int | None = None


class TaskRunner(standard_task_runner.StandardTaskRunner):
    def start(self):
        # there is an issue where the task runner forks a new process
        # this does not play well with the PyTorch model execution
        # TODO: there is still an issue where the task runner is not used in every task
        self.log.info("Starting custom task runner")
        self.process = self._start_by_exec()


_DISABLE_LOAD_STATE: bool = False


@contextlib.contextmanager
def disable_load_state():
    logger.info("Disabling load state")
    global _DISABLE_LOAD_STATE
    __previous_state = _DISABLE_LOAD_STATE
    logger.info(f"Previous state is {__previous_state}")
    _DISABLE_LOAD_STATE = True
    try:
        yield
    finally:
        logger.info(f"Restoring load state to {__previous_state}")
        _DISABLE_LOAD_STATE = __previous_state


def load_state_is_disabled() -> bool:
    return _DISABLE_LOAD_STATE
