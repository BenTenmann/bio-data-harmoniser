import contextlib
import enum
from pathlib import Path
from typing import Any, ClassVar, Optional

import pydantic
import airflow.exceptions

from bio_data_harmoniser.core import utils


class TaskStatus(enum.Enum):
    SUCCESS = "success"
    FAILED = "failed"
    RUNNING = "running"
    SKIPPED = "skipped"
    UNKNOWN = "unknown"

    @classmethod
    def from_state(cls, state: str) -> "TaskStatus":
        if state == "success":
            return TaskStatus.SUCCESS
        elif state == "failed":
            return TaskStatus.FAILED
        elif state == "running":
            return TaskStatus.RUNNING
        elif state == "skipped":
            return TaskStatus.SKIPPED
        return TaskStatus.UNKNOWN


class TaskType(enum.Enum):
    RETRIEVE = "retrieve"
    DOWNLOAD = "download"
    EXTRACT = "extract"
    POOL = "pool"
    PROCESS = "process"


class DecisionType(enum.Enum):
    RETRIEVAL_TYPE_IDENTIFIED = "retrieval_type_identified"
    EXTRACTION_TYPE_IDENTIFIED = "extraction_type_identified"
    URL_RETRIEVED = "url_retrieved"
    FILE_COPIED = "file_copied"
    FILE_FORMAT_IDENTIFIED = "file_format_identified"
    SCHEMA_IDENTIFIED = "schema_identified"
    COLUMN_ALIGNED = "column_aligned"
    UNABLE_TO_PROCESS = "unable_to_process"


class RenameOperation(pydantic.BaseModel):
    original_name: str
    new_name: str


class MappingType(enum.Enum):
    FREE_TEXT = "free_text"
    XREF = "xref"


class MappingOperation(pydantic.BaseModel):
    type: MappingType
    mappings: list[utils.Mapping] = pydantic.Field(default_factory=list)


class ColumnInferenceType(enum.Enum):
    DERIVED = "derived"
    EXTRACTED = "extracted"


class InferenceOperation(pydantic.BaseModel):
    type: ColumnInferenceType
    data: Any


class SetValueOperation(pydantic.BaseModel):
    value: Any


class MapToNullOperation(pydantic.BaseModel):
    values: list[Any]


Operation = RenameOperation | MappingOperation | InferenceOperation | SetValueOperation | MapToNullOperation


class ColumnAlignment(pydantic.BaseModel):
    column_name: str
    operations: list[Operation]


class Decision(pydantic.BaseModel):
    type: DecisionType
    content: str | ColumnAlignment


class Argument(pydantic.BaseModel):
    name: str
    value: str


class NodeMetadata(pydantic.BaseModel):
    name: str
    type: TaskType
    status: TaskStatus = TaskStatus.UNKNOWN
    logs: list[str] = pydantic.Field(default_factory=list)
    decisions: list[Decision] = pydantic.Field(default_factory=list)
    arguments: list[Argument] = pydantic.Field(default_factory=list)
    duration: float = 0.0

    def get_column_alignments_as_dict(self) -> dict[str, ColumnAlignment]:
        return {
            op.content.column_name: op.content
            for op in self.decisions if isinstance(op.content, ColumnAlignment)
        }


class LoggedNode(pydantic.BaseModel):
    id: str
    name: str
    data: NodeMetadata
    upstream_node_ids: list[str] = pydantic.Field(default_factory=list)


class LoggingSession(pydantic.BaseModel):
    node: LoggedNode | None = None
    _session: ClassVar[Optional["LoggingSession"]] = None

    @classmethod
    def get_session(cls) -> "LoggingSession":
        return cls._session or LoggingSession()

    @property
    def node_is_set(self) -> bool:
        return self.node is not None

    def log_decision(self, decision: Decision) -> None:
        if not self.node_is_set:
            return
        self.node.data.decisions.append(decision)

    def log_column_alignment_op(
        self,
        column_name: str,
        operation: Operation
    ) -> None:
        if not self.node_is_set:
            return
        alignments = self.node.data.get_column_alignments_as_dict()
        if column_name not in alignments:
            self.log_decision(
                Decision(
                    type=DecisionType.COLUMN_ALIGNED,
                    content=ColumnAlignment(
                        column_name=column_name,
                        operations=[operation],
                    ),
                )
            )
        else:
            alignments[column_name].operations.append(operation)

    def log_status(self, status: TaskStatus) -> None:
        if not self.node_is_set:
            return
        self.node.data.status = status

    def get_logged_mappings(self) -> list[utils.Mapping]:
        mappings = []
        if not self.node_is_set:
            return mappings
        for decision in self.node.data.decisions:
            if not isinstance(decision.content, ColumnAlignment):
                continue
            for operation in decision.content.operations:
                if isinstance(operation, MappingOperation):
                    mappings.extend(operation.mappings)
        return mappings


@contextlib.contextmanager
def logging_session(node: LoggedNode, output_dir: str | None = None) -> LoggingSession:
    session = LoggingSession(node=node)
    previous_session = LoggingSession._session
    LoggingSession._session = session
    try:
        session.log_status(TaskStatus.RUNNING)
        yield session
    except airflow.exceptions.AirflowSkipException as exc:
        session.log_status(TaskStatus.SKIPPED)
        raise exc
    except Exception as exc:
        session.log_status(TaskStatus.FAILED)
        raise exc
    else:
        session.log_status(TaskStatus.SUCCESS)
    finally:
        if output_dir is not None:
            path = Path(output_dir)
            path.mkdir(parents=True, exist_ok=True)
            (
                (path / f"{session.node.id}.json")
                .write_text(session.node.model_dump_json())
            )
        LoggingSession._session = previous_session
