# Handles and execution

For work that outlives the call that started it. A handle is a value you can
store and come back to from another process — polling it, or cancelling it.

::: gantry.ExecutionHandle

::: gantry.Execution

::: gantry.ExecutionState

::: gantry.ValidationResult

## Running work

The module-level functions act on a process-default control plane. `run`
submits and waits; the others exist for work that outlives the call.

::: gantry.submit

::: gantry.wait

::: gantry.run

::: gantry.get

::: gantry.cancel

::: gantry.configure

::: gantry.register_adapter

## The control plane

Construct one directly to keep adapters and execution history isolated from
the process default — two control planes do not see each other's runs.

::: gantry.ControlPlane

::: gantry.ExecutionStore

::: gantry.RunRecord

::: gantry.MemoryExecutionStore

## Adapters and targets

::: gantry.ExecutionAdapter

::: gantry.ExecutionTarget

::: gantry.AdapterCapabilities

::: gantry.Artifact

::: gantry.Context

::: gantry.Tool

## Admission

The gate between proposing and executing: a policy the adapter cannot enforce
is refused rather than warned about.

::: gantry.PolicyRequirements

::: gantry.admit

::: gantry.AdmissionDecision

## Engine results

What the engine reported, before verification decides whether to accept it.
Most callers read `Result` instead; this is the adapter-facing envelope.

::: gantry.ExecutionResult
