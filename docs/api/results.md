# Results and outputs

What an operation hands back. The question these types exist to keep separate is
"did it run" versus "may the answer be believed" — `status` answers the first,
`verification` the second.

::: gantry.sql.SQLResult

::: gantry.Result

::: gantry.ResultStatus

## Rows and references

An adapter returns rows inline when they are bounded, and a reference when the
engine produced something too large to inline. Which you get depends on the
provider.

::: gantry.sql.InlineRows

::: gantry.OutputRef

::: gantry.OutputKind

## Metrics

::: gantry.ExecutionMetrics

## Verification

::: gantry.VerificationResult

::: gantry.CheckResult

::: gantry.Verifier
