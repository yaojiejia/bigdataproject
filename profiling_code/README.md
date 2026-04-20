# /profiling_code

Data-profiling / exploratory surveying code. These are the scripts I used to
understand each raw dataset *before* writing the ETL in `/etl_code` — record
counts, schema discovery, distinct-value inventories, descriptive statistics
(mean / median / mode), and a couple of RDD map/reduce exercises.

Profiling code is kept separate from the production pipeline so that
(a) re-running ETL doesn't re-run heavy exploratory work, and
(b) the grader can see exactly which scripts produced the numbers quoted
in my project write-up.

Each team member has their own subdirectory.

## Run

From the project root:

```bash
make profile-first    # FirstCode.scala  — schemas + mean/median/mode + RDD work
make profile-counts   # CountRecs.scala  — row counts + distinct-value surveys
```
