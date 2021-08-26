<h1 align="center">
  <br>
  <a href="https://vantage6.ai"><img src="https://github.com/IKNL/guidelines/blob/master/resources/logos/vantage6.png?raw=true" alt="vantage6" width="400"></a>
</h1>

<h3 align=center> A privacy preserving federated learning solution</h3>
[![Docker Image](https://github.com/IKNL/v6-summary-py/actions/workflows/main.yml/badge.svg)](https://github.com/IKNL/v6-summary-py/actions/workflows/main.yml)

# Federated Summary

|:warning: priVAcy preserviNg federaTed leArninG infrastructurE for Secure Insight eXchange (VANTAGE6) |
|------------------|
| This algorithm is part of [VANTAGE6](https://github.com/IKNL/vantage6). A docker build of this algorithm can be obtained from harbor.vantage6.ai/algorithms/dsummary |

Algorithm that is inspired by the `Summary` function in R. It report the `Min`, `Q1`, `Mean`, `Median`, `Q3`, `Max` and number of `Nan` values per column from each `Node`.

## Possible Privacy Issues

🚨 Categorial column with only one category <br />
🚨 `Min` an `Max` for each column is reported <br />
🚨 Column names can be geussed, by trail and error

## Privacy Protection

✔️ If column names do not match nothing else is reported <br />
✔️ If dataset has less that 10 rows, no statistical analysis is performed <br />
✔️ Only statistical results `Min`, `Q1`, `Mean`, `Median`, `Q3`, `Max` and number of `Nan` values per column are reported.

## Usage
```python
from vantage6.client import Client
from pathlib import Path

# Create, athenticate and setup client
client = Client("http://127.0.0.1", 5000, "")
client.authenticate("frank@iknl.nl", "password")
client.setup_encryption(None)

# Define algorithm input
input_ = {
    "master": "true",
    "method":"master",
    "args": [
      {
        "num_awards":"Int64",
        "prog":"category", "math":"Int64"
      }
    ],
    "kwargs": {}
}

# Send the task to the central server
task = client.post_task(
    name="testing",
    image="harbor.vantage6.ai/algorithms/summary",
    collaboration_id=1,
    input_= input_,
    organization_ids=[2]
)

# Retrieve the results
res = client.get_results(task_id=task.get("id")
```

## Test / Develop

You need to have Docker installed.

To Build (assuming you are in the project-directory):
```
docker build -t harbor.vantage6.ai/algorithms/summary .
```

To test/run locally the folder `local` is included in the repository. The following command mounts these files and sets the docker `ENVIROMENT_VARIABLE` `DATABASE_URI`.
```
docker run -e DATABASE_URI=/app/database.csv -v .\local\input.txt:/app/input.txt -v .\local\output.txt:/app/output.txt -v .\local\database.csv:/app/database.csv harbor.vantage6.ai/algorithms/summary
```
