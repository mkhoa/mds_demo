---
description: Trigger a Mage pipeline by name
argument-hint: <pipeline name>
---

Use the `managing-mage-pipelines` skill. Trigger the pipeline: `$ARGUMENTS`

1. Confirm the pipeline exists under `application/mage_ai/mds_demo/pipelines/`.
2. Run `mage run mds_demo $ARGUMENTS` in the `magic` container.
3. Report success or failure; on failure, summarize the error from the output.
