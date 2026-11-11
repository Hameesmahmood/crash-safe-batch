# Crash-safe batch

A small TypeScript library that runs long batch jobs in checkpointed steps and resumes after a crash,
with output identical to a run that was never interrupted.

**Status:** in progress. See the [implementation plan](docs/IMPLEMENTATION_PLAN.md).

## Run the tests

```bash
npm ci
npm test        # needs Docker, for a real Postgres via Testcontainers
```
