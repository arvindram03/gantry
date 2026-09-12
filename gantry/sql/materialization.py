# SPDX-License-Identifier: Apache-2.0
"""Create-only governed SQL materialization for caller-proposed native SQL."""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, replace
from enum import StrEnum
from fnmatch import fnmatchcase
from hashlib import sha256
from typing import TYPE_CHECKING, NoReturn, Protocol, runtime_checkable

from gantry.context import Context
from gantry.evidence import EvidenceBundle, Observation, ObservationSource
from gantry.execution import Execution
from gantry.failure import Failure, FailureKind
from gantry.handle import ExecutionHandle
from gantry.output import OutputKind, OutputRef
from gantry.result import ResultStatus
from gantry.runtime import SubmissionError
from gantry.sql.policy import SQLPolicy
from gantry.sql.schema import Table
from gantry.sql.target import SQLTarget
from gantry.tool import Tool
from gantry.verifier import CheckResult, CheckSource, VerificationResult
from gantry.verify import (
    MaterializationCheck,
    VerificationConflict,
    VerificationInputError,
    VerificationUnsupported,
    check_config,
    detect_conflicts,
    parse_agent_checks,
    sourced_result,
    validate_agent_checks,
    verification_schema,
)

if TYPE_CHECKING:
    from gantry.sql.api import SQLConnection


_PLAN_METADATA = "gantry.sql.materialization.plan"
_DESTINATION_METADATA = "gantry.sql.materialization.destination"
_OPERATION_METADATA = "gantry.sql.materialization.operation"
_DANGEROUS_BODY_WORDS = frozenset(
    {
        "ALTER",
        "CREATE",
        "DELETE",
        "DROP",
        "GRANT",
        "INSERT",
        "MERGE",
        "REVOKE",
        "TRUNCATE",
        "UPDATE",
    }
)
_FROM_TERMINATORS = frozenset(
    {
        "EXCEPT",
        "GROUP",
        "HAVING",
        "INTERSECT",
        "LIMIT",
        "ORDER",
        "QUALIFY",
        "RETURNING",
        "UNION",
        "WHERE",
        "WINDOW",
    }
)


class MaterializationOperation(StrEnum):
    """The two operations v0 materialization allows.

    Both create; neither replaces. A destination that already exists is a
    refusal rather than an overwrite.
    """

    CREATE_TABLE_AS = "CREATE_TABLE_AS"
    CREATE_VIEW_AS = "CREATE_VIEW_AS"


@dataclass(frozen=True, slots=True)
class TableRef:
    """A table name parsed out of a materialization, with its parts kept.

    `qualified_name` joins whichever of catalog, schema and name are present.
    Materialization requires a schema on the destination, so that an agent
    cannot write to whatever the session's search path happens to be.
    """

    name: str
    schema: str | None = None
    catalog: str | None = None

    def __post_init__(self) -> None:
        for field_name in ("name", "schema", "catalog"):
            value = getattr(self, field_name)
            if value is not None and not isinstance(value, str):
                raise TypeError(f"table {field_name} must be a string")
            if value is not None and not value.strip():
                raise ValueError(f"table {field_name} must not be empty")

    @property
    def qualified_name(self) -> str:
        return ".".join(part for part in (self.catalog, self.schema, self.name) if part)


@dataclass(frozen=True, slots=True)
class MaterializationProposal:
    """Caller-proposed SQL before anything has been checked about it.

    Construction only rejects a non-string or blank submission; the naming is
    the point — what arrives here is a proposal, and it is a plan only after
    `parse_materialization` has accepted it.
    """

    sql: str

    def __post_init__(self) -> None:
        if not isinstance(self.sql, str):
            raise TypeError("materialization SQL must be a string")
        if not self.sql.strip():
            raise ValueError("materialization SQL must not be empty")


@dataclass(frozen=True, slots=True)
class MaterializationPlan:
    """What a proposal turned out to be: one create, its destination, sources.

    Returned by `parse_materialization` and checked against
    `MaterializationPolicy`. `sources` is what the body reads, excluding CTE
    names, so the policy matches real tables rather than local aliases.
    """

    operation: MaterializationOperation
    sources: tuple[TableRef, ...]
    destination: TableRef
    replace: bool = False


@dataclass(frozen=True, slots=True)
class MaterializationCapabilities:
    """What an adapter can enforce for materialization specifically.

    Separate from `SQLCapabilities` because materialization needs guarantees a
    query does not: `destination_introspection` is what makes create-only
    checkable, and without it the destination cannot be verified after the job
    runs. All default to false.
    """

    create_table_as: bool = False
    create_view_as: bool = False
    durable_jobs: bool = False
    cancel: bool = False
    estimate_bytes_scanned: bool = False
    destination_introspection: bool = False
    result_reference: bool = False


@runtime_checkable
class MaterializationAdapter(Protocol):
    """Additional adapter operation required only for materialization.

    `inspect_table` is how create-only is enforced and how the destination is
    verified afterwards: returning `None` for a table that does not exist is
    what distinguishes "not created" from "created empty". An adapter without
    this cannot materialize.
    """

    """Additional adapter operation required only for materialization."""

    async def inspect_table(
        self,
        reference: TableRef,
        target: SQLTarget,
        *,
        include_row_count: bool = False,
    ) -> Table | None: ...


@runtime_checkable
class ColumnStatistics(Protocol):
    """An adapter that can measure per-column statistics at a destination.

    Separate from `MaterializationAdapter` because not every provider can do
    this, and the spec is explicit that not every provider must support every
    check. An adapter without it makes `null_rate` unsupported, which fails
    closed rather than passing unmeasured.
    """

    async def column_null_rates(
        self,
        reference: TableRef,
        target: SQLTarget,
        columns: Sequence[str],
    ) -> Mapping[str, float]: ...


@dataclass(frozen=True, slots=True)
class MaterializationPolicy:
    """Where a materialization may read from and write to.

    `destinations` must be non-empty — there is no default place to write — and
    patterns are `fnmatch`-style, lower-cased at construction. `create_only` is
    fixed at `True` in v0 and passing anything else raises, so replacement
    cannot be enabled by a config change before the semantics exist.
    """

    sources: Collection[str]
    destinations: Collection[str]
    create_only: bool = True
    timeout_seconds: float = 300
    max_bytes_scanned: int | None = None
    max_cost_usd: float | None = None

    def __post_init__(self) -> None:
        for field_name in ("sources", "destinations"):
            values = getattr(self, field_name)
            if isinstance(values, str):
                raise TypeError(f"{field_name} must be a collection of patterns, not a string")
            if any(not isinstance(value, str) for value in values):
                raise TypeError(f"{field_name} must contain only strings")
            if any(not value.strip() for value in values):
                raise ValueError(f"{field_name} must not contain empty patterns")
            object.__setattr__(self, field_name, tuple(value.lower() for value in values))
        if not self.destinations:
            raise ValueError("at least one materialization destination must be allowed")
        if self.create_only is not True:
            raise ValueError("SQL materialization v0 supports create_only=True only")
        if isinstance(self.timeout_seconds, bool) or not isinstance(
            self.timeout_seconds, (int, float)
        ):
            raise TypeError("timeout must be numeric")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout must be positive")
        if self.max_bytes_scanned is not None and (
            isinstance(self.max_bytes_scanned, bool) or not isinstance(self.max_bytes_scanned, int)
        ):
            raise TypeError("max bytes scanned must be an integer")
        if self.max_bytes_scanned is not None and self.max_bytes_scanned < 0:
            raise ValueError("max bytes scanned must not be negative")
        if self.max_cost_usd is not None and (
            isinstance(self.max_cost_usd, bool) or not isinstance(self.max_cost_usd, (int, float))
        ):
            raise TypeError("max cost must be numeric")
        if self.max_cost_usd is not None and self.max_cost_usd < 0:
            raise ValueError("max cost must not be negative")


@dataclass(frozen=True, slots=True)
class MaterializationResult:
    """The outcome of a materialization: the destination, and whether to trust it.

    `ok` is `ACCEPTED` only, which requires verification to have passed — a
    `CREATE TABLE AS` that ran and produced the wrong table is not accepted.
    `uri` names the destination when one was created.
    """

    status: ResultStatus
    output: OutputRef | None = None
    execution: Execution | None = None
    verification: VerificationResult | None = None
    failure: Failure | None = None
    evidence: EvidenceBundle | None = None
    """What Gantry observed while deciding, serializable and outliving the run."""

    @property
    def ok(self) -> bool:
        return self.status is ResultStatus.ACCEPTED

    @property
    def handle(self) -> ExecutionHandle | None:
        return None if self.execution is None else self.execution.handle

    @property
    def uri(self) -> str | None:
        """Return the materialized destination URI when one is available."""

        return None if self.output is None else self.output.uri


class MaterializationError(Exception):
    """Raised when a proposal is refused, carrying why and at what stage.

    `failure` is the normalized reason and `status` the outcome it maps to,
    defaulting to `REJECTED` — the common case, where nothing ran. It is an
    exception rather than a returned value because a refused proposal has no
    result to describe.
    """

    def __init__(self, failure: Failure, status: ResultStatus = ResultStatus.REJECTED) -> None:
        self.failure = failure
        self.status = status
        super().__init__(failure.message)


@dataclass(frozen=True, slots=True)
class _Token:
    value: str
    kind: str
    quote: str | None = None

    @property
    def upper(self) -> str:
        return self.value.upper() if self.kind == "word" else self.value


class SQLMaterializer:
    """Credential-free, create-only SQL materializer."""

    name = "materialize_sql"
    description = "Create a governed derived table from approved SQL sources."
    _base_agent_capabilities = frozenset(
        {
            "destination_exists",
            "not_empty",
            "row_count",
            "required_columns",
            "null_rate",
        }
    )

    @property
    def _agent_capabilities(self) -> frozenset[str]:
        capabilities = self._base_agent_capabilities
        if not isinstance(self._connection._materialization_adapter(), ColumnStatistics):
            capabilities = capabilities - {"null_rate"}
        return capabilities

    def __init__(
        self,
        connection: SQLConnection,
        policy: MaterializationPolicy,
        verify: Sequence[MaterializationCheck],
    ) -> None:
        if any(not isinstance(check, MaterializationCheck) for check in verify):
            raise TypeError("verify must contain materialization verification checks")
        self._connection = connection
        self._policy = policy
        self._trusted_checks = tuple(verify)
        self._plans: dict[str, MaterializationPlan] = {}
        self._proposals: dict[str, str] = {}
        self._agent_checks: dict[str, tuple[MaterializationCheck, ...]] = {}

    @property
    def input_schema(self) -> dict[str, object]:
        return {
            "type": "object",
            "properties": {
                "sql": {"type": "string"},
                "verify": verification_schema(self._agent_capabilities),
            },
            "required": ["sql"],
            "additionalProperties": False,
        }

    def tool(
        self,
        *,
        name: str = "materialize_sql",
        description: str = "Create a derived table from approved SQL sources.",
    ) -> Tool[MaterializationResult]:
        """Return the narrow framework-neutral form of this operation."""

        return Tool(
            name=name,
            description=description,
            input_schema=self.input_schema,
            _handler=self._invoke_tool,
        )

    @property
    def capabilities(self) -> MaterializationCapabilities:
        capabilities = self._connection.capabilities()
        return MaterializationCapabilities(
            create_table_as=capabilities.create_table_as,
            create_view_as=capabilities.create_view_as,
            durable_jobs=capabilities.reconnect,
            cancel=capabilities.cancellation,
            estimate_bytes_scanned=capabilities.bytes_scanned,
            destination_introspection=capabilities.destination_introspection,
            result_reference=capabilities.materialization_reference,
        )

    def inspect(self, proposal: str | MaterializationProposal) -> MaterializationPlan:
        value = (
            proposal
            if isinstance(proposal, MaterializationProposal)
            else MaterializationProposal(proposal)
        )
        return parse_materialization(value.sql)

    async def submit(
        self,
        proposal: str | MaterializationProposal,
        *,
        verify: Sequence[MaterializationCheck] = (),
    ) -> ExecutionHandle:
        value = (
            proposal
            if isinstance(proposal, MaterializationProposal)
            else MaterializationProposal(proposal)
        )
        agent_checks = validate_agent_checks(verify, capabilities=self._agent_capabilities)
        detect_conflicts(self._trusted_checks, agent_checks)
        plan = self.inspect(value)
        await self._admit(plan)
        context = Context(metadata={_PLAN_METADATA: plan})
        try:
            handle = await self._connection.submit(
                value.sql,
                policy=self._sql_policy(),
                context=context,
            )
        except SubmissionError as error:
            failure = error.result.failure or Failure(
                FailureKind.SUBMISSION_ERROR,
                False,
                "materialization submission failed",
            )
            raise MaterializationError(
                _normalize_submission_failure(failure),
                error.result.status,
            ) from error
        self._plans[handle.gantry_id] = plan
        # Identifies the proposal without storing it. Evidence should say which
        # statement was run without becoming a place agent-written SQL
        # accumulates, and a digest is enough to match a run to its proposal.
        proposal_hash = sha256(value.sql.encode()).hexdigest()
        handle = replace(
            handle,
            metadata={
                **handle.metadata,
                "gantry.proposal_hash": proposal_hash,
                "gantry.agent_verification": [check_config(check) for check in agent_checks],
            },
        )
        self._proposals[handle.gantry_id] = proposal_hash
        self._agent_checks[handle.gantry_id] = agent_checks  # type: ignore[assignment]
        return handle

    async def wait(
        self,
        handle: ExecutionHandle,
        *,
        poll_interval_seconds: float = 1.0,
    ) -> MaterializationResult:
        plan = self._plans.get(handle.gantry_id) or _plan_from_handle(handle)
        if plan is None:
            return _failed(
                ResultStatus.UNKNOWN,
                Failure(
                    FailureKind.UNKNOWN,
                    False,
                    "execution handle does not identify a materialization destination",
                ),
            )
        sql_result = await self._connection.wait(
            handle,
            poll_interval_seconds=poll_interval_seconds,
        )
        execution = await self._connection.status(handle)
        if not sql_result.ok:
            verification = VerificationResult.passed()
            result = MaterializationResult(
                status=sql_result.status,
                execution=execution,
                failure=sql_result.failure,
                evidence=_evidence(
                    plan,
                    execution,
                    None,
                    verification,
                    sql_result.status,
                    self._proposal_hash(handle),
                    self._agent_checks_for(handle),
                ),
            )
            return _recorded(result)

        output = _materialized_output(sql_result.outputs, plan.destination)
        if output is None:
            result = MaterializationResult(
                status=ResultStatus.FAILED,
                execution=execution,
                failure=Failure(
                    FailureKind.ENGINE_ERROR,
                    False,
                    "adapter did not return a reference to the materialized destination",
                ),
                evidence=_evidence(
                    plan,
                    execution,
                    None,
                    VerificationResult.passed(),
                    ResultStatus.FAILED,
                    self._proposal_hash(handle),
                    self._agent_checks_for(handle),
                ),
            )
            return _recorded(result)

        agent_checks = self._agent_checks_for(handle)
        verification = await self._verification(plan.destination, agent_checks)
        if not verification.ok:
            unsupported = verification.unsupported_checks
            reasons = unsupported or verification.failed_checks
            message = next(
                (check.message for check in reasons if check.message),
                "materialization verification failed",
            )
            status = ResultStatus.VERIFICATION_FAILED
            result = MaterializationResult(
                status=status,
                output=output,
                execution=execution,
                verification=verification,
                failure=Failure(
                    FailureKind.UNSUPPORTED_VERIFICATION
                    if unsupported
                    else FailureKind.VERIFICATION_FAILED,
                    False,
                    message,
                ),
                evidence=_evidence(
                    plan,
                    execution,
                    output,
                    verification,
                    status,
                    self._proposal_hash(handle),
                    agent_checks,
                ),
            )
            return _recorded(result)
        result = MaterializationResult(
            status=ResultStatus.ACCEPTED,
            output=output,
            execution=execution,
            verification=verification,
            evidence=_evidence(
                plan,
                execution,
                output,
                verification,
                ResultStatus.ACCEPTED,
                self._proposal_hash(handle),
                agent_checks,
            ),
        )
        return _recorded(result)

    async def status(self, handle: ExecutionHandle) -> Execution:
        return await self._connection.status(handle)

    async def cancel(self, handle: ExecutionHandle, *, mode: str = "default") -> Execution:
        return await self._connection.cancel(handle, mode=mode)

    async def __call__(
        self,
        proposal: str | MaterializationProposal,
        *,
        verify: Sequence[MaterializationCheck] = (),
    ) -> MaterializationResult:
        try:
            handle = await self.submit(proposal, verify=verify)
        except VerificationUnsupported as error:
            return _failed(
                ResultStatus.VERIFICATION_UNSUPPORTED,
                Failure(FailureKind.VERIFICATION_UNSUPPORTED, False, str(error)),
            )
        except VerificationConflict as error:
            return _failed(
                ResultStatus.VERIFICATION_CONFLICT,
                Failure(FailureKind.VERIFICATION_CONFLICT, False, str(error)),
            )
        except VerificationInputError as error:
            return _failed(
                ResultStatus.REJECTED,
                Failure(FailureKind.VALIDATION_ERROR, False, str(error)),
            )
        except MaterializationError as error:
            return _failed(error.status, error.failure)
        except (TypeError, ValueError) as error:
            return _failed(
                ResultStatus.REJECTED,
                Failure(FailureKind.VALIDATION_ERROR, False, str(error)),
            )
        return await self.wait(handle, poll_interval_seconds=0.05)

    async def _invoke_tool(self, arguments: Mapping[str, object]) -> MaterializationResult:
        unexpected = set(arguments) - {"sql", "verify"}
        if unexpected:
            names = ", ".join(sorted(unexpected))
            raise ValueError(f"unexpected materialization tool arguments: {names}")
        sql = arguments.get("sql")
        if not isinstance(sql, str):
            raise TypeError("sql must be a string")
        try:
            checks = parse_agent_checks(
                arguments.get("verify"), capabilities=self._agent_capabilities
            )
        except VerificationUnsupported as error:
            return _failed(
                ResultStatus.VERIFICATION_UNSUPPORTED,
                Failure(FailureKind.VERIFICATION_UNSUPPORTED, False, str(error)),
            )
        except VerificationInputError as error:
            return _failed(
                ResultStatus.REJECTED,
                Failure(FailureKind.VALIDATION_ERROR, False, str(error)),
            )
        return await self(sql, verify=checks)  # type: ignore[arg-type]

    async def _admit(self, plan: MaterializationPlan) -> None:
        capabilities = self._connection.capabilities()
        if (
            plan.operation is MaterializationOperation.CREATE_TABLE_AS
            and not capabilities.create_table_as
        ):
            _reject(FailureKind.OPERATION_NOT_ALLOWED, "adapter does not support CREATE TABLE AS")
        if (
            plan.operation is MaterializationOperation.CREATE_VIEW_AS
            and not capabilities.create_view_as
        ):
            _reject(FailureKind.OPERATION_NOT_ALLOWED, "adapter does not support CREATE VIEW AS")
        if not capabilities.write_execution:
            _reject(
                FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
                "adapter cannot enforce governed write execution",
            )
        if not capabilities.materialization_reference:
            _reject(
                FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
                "adapter cannot return a materialized output reference",
            )
        if not capabilities.destination_introspection:
            _reject(
                FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
                "adapter cannot inspect materialization destinations",
            )
        if self._policy.max_bytes_scanned is not None and not capabilities.bytes_scanned:
            _reject(
                FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
                "adapter cannot enforce maximum bytes scanned",
            )
        if self._policy.max_cost_usd is not None and not capabilities.cost_limit:
            _reject(
                FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
                "adapter cannot enforce maximum cost",
            )
        disallowed = [
            source for source in plan.sources if not _matches(source, self._policy.sources)
        ]
        if disallowed:
            names = ", ".join(source.qualified_name for source in disallowed)
            _reject(FailureKind.SOURCE_NOT_ALLOWED, f"source is not allowed: {names}")
        if not _matches(plan.destination, self._policy.destinations):
            _reject(
                FailureKind.DESTINATION_NOT_ALLOWED,
                f"destination is not allowed: {plan.destination.qualified_name}",
            )
        try:
            existing = await self._connection._inspect_table(plan.destination)
        except NotImplementedError as error:
            _reject(
                FailureKind.UNSUPPORTED_POLICY_REQUIREMENT,
                f"adapter cannot inspect materialization destinations: {error}",
            )
        except Exception as error:
            _reject(
                FailureKind.VALIDATION_ERROR,
                f"destination inspection raised {type(error).__name__}: {error}",
            )
        if existing is not None:
            _reject(
                FailureKind.DESTINATION_EXISTS,
                f"destination already exists: {plan.destination.qualified_name}",
            )

    async def _verification(
        self,
        destination: TableRef,
        agent_checks: Sequence[MaterializationCheck] = (),
    ) -> VerificationResult:
        effective = (*self._trusted_checks, *agent_checks)
        if not effective:
            return VerificationResult.passed()
        try:
            table = await self._connection._inspect_table(
                destination,
                include_row_count=any(
                    getattr(verifier, "requires_row_count", False) for verifier in effective
                ),
            )
        except Exception as error:
            failure = VerificationResult.failed(
                f"destination inspection raised {type(error).__name__}: {error}",
                name="destination_introspection",
            )
            check = sourced_result(
                failure.checks[0],
                CheckSource.TRUSTED,
                observation_source="destination",
            )
            return replace(failure, checks=(check,))
        table = await self._with_null_rates(table, destination, effective)
        checks: list[CheckResult] = []
        for index, verifier in enumerate(effective):
            source = CheckSource.TRUSTED if index < len(self._trusted_checks) else CheckSource.AGENT
            try:
                checks.append(
                    sourced_result(
                        verifier.evaluate(table), source, observation_source="destination"
                    )
                )
            except Exception as error:
                checks.append(
                    CheckResult(
                        name=type(verifier).__name__,
                        ok=False,
                        message=f"verification raised {type(error).__name__}: {error}",
                        source=source,
                    )
                )
        return VerificationResult(
            ok=all(check.ok for check in checks),
            checks=tuple(checks),
        )

    def _proposal_hash(self, handle: ExecutionHandle) -> str | None:
        stored = handle.metadata.get("gantry.proposal_hash")
        return self._proposals.get(handle.gantry_id) or (
            stored if isinstance(stored, str) else None
        )

    def _agent_checks_for(self, handle: ExecutionHandle) -> tuple[MaterializationCheck, ...]:
        local = self._agent_checks.get(handle.gantry_id)
        if local is not None:
            return local
        try:
            checks = parse_agent_checks(
                handle.metadata.get("gantry.agent_verification"),
                capabilities=self._agent_capabilities,
            )
        except VerificationInputError:
            return ()
        return checks  # type: ignore[return-value]

    async def _with_null_rates(
        self,
        table: Table | None,
        destination: TableRef,
        checks: Sequence[MaterializationCheck],
    ) -> Table | None:
        """Attach null rates to the destination metadata, if any check needs them.

        Gathered only when asked for: it is a scan per column, and a check that
        nobody configured should cost nothing.
        """
        wanted: list[str] = []
        for verifier in checks:
            wanted.extend(getattr(verifier, "null_rate_columns", ()))
        if table is None or not wanted:
            return table
        adapter = self._connection._materialization_adapter()
        if not isinstance(adapter, ColumnStatistics):
            # Leaves the metadata absent, which makes the check unsupported.
            return table
        try:
            rates = await adapter.column_null_rates(
                destination, self._connection._target, tuple(dict.fromkeys(wanted))
            )
        except Exception:
            return table
        return replace(table, metadata={**table.metadata, "null_rates": dict(rates)})

    def _sql_policy(self) -> SQLPolicy:
        return SQLPolicy(
            read_only=False,
            max_rows=1,
            timeout_seconds=self._policy.timeout_seconds,
            max_bytes_scanned=self._policy.max_bytes_scanned,
            max_cost_usd=self._policy.max_cost_usd,
        )


def parse_materialization(sql: str) -> MaterializationPlan:
    """Parse caller-proposed SQL into a create-only `MaterializationPlan`.

    Accepts exactly one `CREATE TABLE ... AS` or `CREATE VIEW ... AS` whose
    destination names a schema, and returns the operation, the schema-qualified
    destination, and the source tables the body reads. Raises
    `MaterializationError` for anything else: several statements, a non-create
    operation, `OR REPLACE` or `IF NOT EXISTS`, an unqualified destination, a
    body that is not a `SELECT`/`WITH` query, or a body carrying a second
    effect such as `DELETE` or `DROP`.
    """
    proposal = MaterializationProposal(sql)
    statements = _split_statements(proposal.sql)
    if len(statements) != 1:
        _reject(FailureKind.OPERATION_NOT_ALLOWED, "materialization must contain one SQL statement")
    tokens = _tokenize(statements[0])
    if not tokens or tokens[0].upper != "CREATE":
        _reject(
            FailureKind.OPERATION_NOT_ALLOWED,
            "only CREATE TABLE AS or CREATE VIEW AS is allowed",
        )

    index = 1
    replace = False
    if _words_at(tokens, index, "OR", "REPLACE"):
        replace = True
        index += 2
    if index >= len(tokens) or tokens[index].upper not in {"TABLE", "VIEW"}:
        _reject(
            FailureKind.OPERATION_NOT_ALLOWED,
            "only CREATE TABLE AS or CREATE VIEW AS is allowed",
        )
    object_type = tokens[index].upper
    index += 1
    if _words_at(tokens, index, "IF", "NOT", "EXISTS"):
        _reject(FailureKind.OPERATION_NOT_ALLOWED, "IF NOT EXISTS is not allowed")
    destination, index = _parse_ref(tokens, index)
    if destination.schema is None:
        _reject(
            FailureKind.DESTINATION_NOT_ALLOWED,
            "materialization destination must include a schema or dataset",
        )
    if replace:
        _reject(FailureKind.OPERATION_NOT_ALLOWED, "replacement materialization is not allowed")
    if index >= len(tokens) or tokens[index].upper != "AS":
        _reject(
            FailureKind.OPERATION_NOT_ALLOWED,
            "materialization must use CREATE TABLE AS or CREATE VIEW AS",
        )
    body_start = index + 1
    if body_start >= len(tokens) or tokens[body_start].upper not in {"SELECT", "WITH"}:
        _reject(FailureKind.OPERATION_NOT_ALLOWED, "materialization body must be a SELECT query")
    for token in tokens[body_start:]:
        if token.kind == "word" and token.upper in _DANGEROUS_BODY_WORDS:
            _reject(
                FailureKind.OPERATION_NOT_ALLOWED,
                f"{token.upper} is not allowed inside a materialization query",
            )
    ctes = _cte_names(tokens, body_start)
    sources = _source_refs(tokens, body_start, ctes)
    operation = (
        MaterializationOperation.CREATE_TABLE_AS
        if object_type == "TABLE"
        else MaterializationOperation.CREATE_VIEW_AS
    )
    return MaterializationPlan(operation, sources, destination, replace=False)


def _source_refs(
    tokens: Sequence[_Token],
    start: int,
    ctes: frozenset[str],
) -> tuple[TableRef, ...]:
    sources: list[TableRef] = []
    seen: set[str] = set()
    depths = _depths(tokens)
    for index in range(start, len(tokens)):
        if tokens[index].upper not in {"FROM", "JOIN"}:
            continue
        source_index = index + 1
        if source_index >= len(tokens):
            _reject(FailureKind.VALIDATION_ERROR, "source reference is incomplete")
        if tokens[source_index].upper == "LATERAL":
            source_index += 1
        if source_index >= len(tokens):
            _reject(FailureKind.VALIDATION_ERROR, "source reference is incomplete")
        if tokens[source_index].value == "(":
            continue
        reference, end = _parse_ref(tokens, source_index)
        if end < len(tokens) and tokens[end].value == "(":
            _reject(
                FailureKind.OPERATION_NOT_ALLOWED,
                "table-valued source expressions are not supported in materialization v0",
            )
        _reject_comma_sources(tokens, end, depths[index])
        if (
            reference.schema is None
            and reference.catalog is None
            and reference.name.lower() in ctes
        ):
            continue
        qualified = reference.qualified_name.lower()
        if qualified not in seen:
            seen.add(qualified)
            sources.append(reference)
    return tuple(sources)


def _reject_comma_sources(tokens: Sequence[_Token], start: int, depth: int) -> None:
    for index in range(start, len(tokens)):
        token = tokens[index]
        token_depth = _depth_at(tokens, index)
        if token_depth < depth:
            return
        if token_depth == depth and token.kind == "word" and token.upper in _FROM_TERMINATORS:
            return
        if token_depth == depth and token.upper in {"JOIN", "ON"}:
            return
        if token_depth == depth and token.value == ",":
            _reject(
                FailureKind.OPERATION_NOT_ALLOWED,
                "comma-separated SQL sources are not supported; use explicit JOIN",
            )


def _cte_names(tokens: Sequence[_Token], start: int) -> frozenset[str]:
    if tokens[start].upper != "WITH":
        return frozenset()
    names: set[str] = set()
    index = start + 1
    if index < len(tokens) and tokens[index].upper == "RECURSIVE":
        index += 1
    while index < len(tokens):
        if not _identifier(tokens[index]):
            _reject(FailureKind.VALIDATION_ERROR, "could not inspect materialization CTE")
        names.add(tokens[index].value.lower())
        index += 1
        if index < len(tokens) and tokens[index].value == "(":
            index = _after_balanced(tokens, index)
        if index >= len(tokens) or tokens[index].upper != "AS":
            _reject(FailureKind.VALIDATION_ERROR, "could not inspect materialization CTE")
        index += 1
        if index >= len(tokens) or tokens[index].value != "(":
            _reject(FailureKind.VALIDATION_ERROR, "could not inspect materialization CTE")
        index = _after_balanced(tokens, index)
        if index >= len(tokens) or tokens[index].value != ",":
            break
        index += 1
    return frozenset(names)


def _parse_ref(tokens: Sequence[_Token], start: int) -> tuple[TableRef, int]:
    if start >= len(tokens) or not _identifier(tokens[start]):
        _reject(FailureKind.VALIDATION_ERROR, "could not inspect SQL object reference")
    first = tokens[start]
    parts = first.value.split(".") if first.quote == "`" and "." in first.value else [first.value]
    index = start + 1
    while index + 1 < len(tokens) and tokens[index].value == ".":
        if not _identifier(tokens[index + 1]):
            _reject(FailureKind.VALIDATION_ERROR, "could not inspect SQL object reference")
        parts.append(tokens[index + 1].value)
        index += 2
    if not 1 <= len(parts) <= 3 or any(not part for part in parts):
        _reject(FailureKind.VALIDATION_ERROR, "SQL object names must contain one to three parts")
    if len(parts) == 3:
        return TableRef(parts[2], parts[1], parts[0]), index
    if len(parts) == 2:
        return TableRef(parts[1], parts[0]), index
    return TableRef(parts[0]), index


def _matches(reference: TableRef, patterns: Collection[str]) -> bool:
    qualified = reference.qualified_name.lower()
    candidates = [qualified]
    if reference.catalog is not None and reference.schema is not None:
        candidates.append(f"{reference.schema}.{reference.name}".lower())
    return any(fnmatchcase(candidate, pattern) for pattern in patterns for candidate in candidates)


def _recorded(result: MaterializationResult) -> MaterializationResult:
    from gantry.runs import record

    record(result.evidence)
    return result


def _evidence(
    plan: MaterializationPlan,
    execution: Execution,
    output: OutputRef | None,
    verification: VerificationResult,
    decision: ResultStatus,
    proposal_hash: str | None = None,
    agent_checks: Sequence[MaterializationCheck] = (),
) -> EvidenceBundle:
    """Assemble the record of what was seen, from the three sources.

    The engine's account of itself, the measurements taken at the destination,
    and what Gantry was configured to require. Each observation keeps its
    source, because they are not equally trustworthy: a runtime the engine
    reports is its own word for it, while a row count is something Gantry went
    and looked at.
    """
    handle = execution.handle
    observations: list[Observation] = [
        Observation("execution_state", execution.state.value, ObservationSource.ENGINE),
    ]
    metrics = execution.metrics
    for name, value, unit in (
        ("rows_read", metrics.rows_read, "rows"),
        ("rows_written", metrics.rows_written, "rows"),
        ("bytes_read", metrics.bytes_read, "bytes"),
        ("runtime_seconds", metrics.runtime_seconds, "seconds"),
    ):
        if value is not None:
            observations.append(Observation(name, value, ObservationSource.ENGINE, unit=unit))
    for check in verification.checks:
        # The measurement the check was decided on, kept beside the verdict so
        # a reader can disagree with the bound without rerunning anything.
        if check.actual is not None:
            observations.append(Observation(check.name, check.actual, ObservationSource.OUTPUT))
    return EvidenceBundle(
        run_id=handle.gantry_id,
        engine=handle.engine,
        operation=plan.operation.value,
        decision=decision.value,
        native_execution_id=handle.native_id,
        proposal_hash=proposal_hash,
        inputs=tuple(source.qualified_name for source in plan.sources),
        outputs=() if output is None else (output.uri,),
        started_at=execution.started_at or handle.submitted_at,
        finished_at=execution.updated_at,
        execution={
            "status": execution.state.value,
            "engine": handle.engine,
            "target": handle.target,
        },
        proposal={
            "hash": proposal_hash,
            "agent_verification": [check_config(check) for check in agent_checks],
        },
        observations=tuple(observations),
        checks=verification.checks,
    )


def _materialized_output(
    outputs: Sequence[OutputRef],
    destination: TableRef,
) -> OutputRef | None:
    referenced = tuple(output for output in outputs if output.kind is not OutputKind.INLINE)
    destination_name = destination.qualified_name.lower().replace(".", "/")
    return next(
        (output for output in referenced if destination_name in output.uri.lower()),
        referenced[0] if referenced else None,
    )


def _plan_from_handle(handle: ExecutionHandle) -> MaterializationPlan | None:
    destination = handle.metadata.get(_DESTINATION_METADATA)
    operation = handle.metadata.get(_OPERATION_METADATA)
    if not isinstance(destination, str) or not isinstance(operation, str):
        return None
    try:
        reference, end = _parse_ref(_tokenize(destination), 0)
        if end != len(_tokenize(destination)):
            return None
        normalized = MaterializationOperation(operation)
    except (MaterializationError, ValueError):
        return None
    return MaterializationPlan(normalized, (), reference)


def _normalize_submission_failure(failure: Failure) -> Failure:
    lowered = failure.message.lower()
    if "estimated bytes" in lowered or "maximum bytes" in lowered or "cost" in lowered:
        return Failure(
            FailureKind.COST_LIMIT_EXCEEDED,
            failure.retryable,
            failure.message,
            failure.native_code,
            failure.native_message,
            failure.native,
        )
    return failure


def _failed(status: ResultStatus, failure: Failure) -> MaterializationResult:
    return MaterializationResult(status=status, failure=failure)


def _reject(kind: FailureKind, message: str) -> NoReturn:
    raise MaterializationError(Failure(kind, False, message))


def _words_at(tokens: Sequence[_Token], start: int, *words: str) -> bool:
    return len(tokens) >= start + len(words) and all(
        tokens[start + offset].upper == word for offset, word in enumerate(words)
    )


def _identifier(token: _Token) -> bool:
    return token.kind in {"word", "identifier"}


def _after_balanced(tokens: Sequence[_Token], start: int) -> int:
    depth = 0
    for index in range(start, len(tokens)):
        if tokens[index].value == "(":
            depth += 1
        elif tokens[index].value == ")":
            depth -= 1
            if depth == 0:
                return index + 1
    _reject(FailureKind.VALIDATION_ERROR, "unbalanced parentheses in materialization SQL")


def _depths(tokens: Sequence[_Token]) -> tuple[int, ...]:
    values: list[int] = []
    depth = 0
    for token in tokens:
        values.append(depth)
        if token.value == "(":
            depth += 1
        elif token.value == ")":
            depth -= 1
            if depth < 0:
                _reject(
                    FailureKind.VALIDATION_ERROR, "unbalanced parentheses in materialization SQL"
                )
    if depth:
        _reject(FailureKind.VALIDATION_ERROR, "unbalanced parentheses in materialization SQL")
    return tuple(values)


def _depth_at(tokens: Sequence[_Token], index: int) -> int:
    depth = 0
    for token in tokens[:index]:
        if token.value == "(":
            depth += 1
        elif token.value == ")":
            depth -= 1
    return depth


def _split_statements(sql: str) -> tuple[str, ...]:
    statements: list[str] = []
    start = 0
    quote: str | None = None
    index = 0
    while index < len(sql):
        char = sql[index]
        if quote is not None:
            closing = "]" if quote == "[" else quote
            if char == closing:
                if index + 1 < len(sql) and sql[index + 1] == closing:
                    index += 1
                else:
                    quote = None
        elif char in {"'", '"', "`", "["}:
            quote = char
        elif char == "-" and index + 1 < len(sql) and sql[index + 1] == "-":
            newline = sql.find("\n", index + 2)
            index = len(sql) if newline == -1 else newline
        elif char == "/" and index + 1 < len(sql) and sql[index + 1] == "*":
            end = sql.find("*/", index + 2)
            index = len(sql) if end == -1 else end + 1
        elif char == ";":
            statement = sql[start:index].strip()
            if statement:
                statements.append(statement)
            start = index + 1
        index += 1
    final = sql[start:].strip()
    if final:
        statements.append(final)
    return tuple(statements)


def _tokenize(sql: str) -> tuple[_Token, ...]:
    tokens: list[_Token] = []
    index = 0
    while index < len(sql):
        char = sql[index]
        if char.isspace():
            index += 1
            continue
        if sql.startswith("--", index):
            newline = sql.find("\n", index + 2)
            index = len(sql) if newline == -1 else newline + 1
            continue
        if sql.startswith("/*", index):
            end = sql.find("*/", index + 2)
            if end == -1:
                _reject(FailureKind.VALIDATION_ERROR, "unterminated SQL comment")
            index = end + 2
            continue
        if char == "'":
            _, index = _quoted(sql, index, "'", "'")
            tokens.append(_Token("", "literal", "'"))
            continue
        if char in {'"', "`", "["}:
            closing = "]" if char == "[" else char
            value, index = _quoted(sql, index, char, closing)
            tokens.append(_Token(value, "identifier", char))
            continue
        word = re.match(r"[A-Za-z_][A-Za-z0-9_$-]*", sql[index:])
        if word is not None:
            value = word.group(0)
            tokens.append(_Token(value, "word"))
            index += len(value)
            continue
        if char.isdigit():
            number = re.match(r"[0-9]+(?:\.[0-9]+)?", sql[index:])
            assert number is not None
            value = number.group(0)
            tokens.append(_Token(value, "literal"))
            index += len(value)
            continue
        tokens.append(_Token(char, "symbol"))
        index += 1
    return tuple(tokens)


def _quoted(sql: str, start: int, opening: str, closing: str) -> tuple[str, int]:
    values: list[str] = []
    index = start + 1
    while index < len(sql):
        if sql[index] == closing:
            if index + 1 < len(sql) and sql[index + 1] == closing:
                values.append(closing)
                index += 2
                continue
            return "".join(values), index + 1
        values.append(sql[index])
        index += 1
    _reject(FailureKind.VALIDATION_ERROR, f"unterminated {opening} quote")


__all__ = [
    "MaterializationAdapter",
    "MaterializationCapabilities",
    "MaterializationError",
    "MaterializationOperation",
    "MaterializationPlan",
    "MaterializationPolicy",
    "MaterializationProposal",
    "MaterializationResult",
    "SQLMaterializer",
    "TableRef",
    "parse_materialization",
]
