# Failures

Every adapter's errors are normalized into one taxonomy, so calling code can
decide what to do without knowing which engine produced the problem.

The distinction that matters most is **retryable**: a submission that never
reached the engine can be retried safely, and a statement the engine refused on
its own terms cannot — retrying it reproduces it.

::: gantry.Failure

::: gantry.FailureKind

::: gantry.SubmissionError
