"""Logical schema of a Dataset."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, model_validator

from gantry.core.names import FieldName


class FieldSchema(BaseModel):
    """One field. `type` is the source-declared type, carried verbatim.

    Cross-engine type mapping is an adapter concern (Day 8), not a modelling
    concern - normalising too early loses the information adapters need.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: FieldName
    type: str
    nullable: bool = True


class DatasetSchema(BaseModel):
    """Keys, time semantics and (once discovered) fields.

    A manifest may declare keys and a time field before any field list exists:
    discovery fills `fields` in later, and the manifest is versioned when it does.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    keys: tuple[FieldName, ...] = ()
    time_field: FieldName | None = None
    fields: tuple[FieldSchema, ...] = ()

    @model_validator(mode="after")
    def _check_consistency(self) -> DatasetSchema:
        if len(set(self.keys)) != len(self.keys):
            raise ValueError(f"duplicate key fields: {self.keys}")

        if not self.fields:
            return self

        known = {field.name for field in self.fields}
        if len(known) != len(self.fields):
            raise ValueError("duplicate field names")

        missing = [key for key in self.keys if key not in known]
        if missing:
            raise ValueError(f"key fields not present in schema: {missing}")
        if self.time_field is not None and self.time_field not in known:
            raise ValueError(f"time_field {self.time_field!r} not present in schema")
        return self

    def field(self, name: str) -> FieldSchema | None:
        return next((f for f in self.fields if f.name == name), None)
