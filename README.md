JGR DE Assessment

Workflow:

1. Read data from SMTSample.json
2. "Clean" data from SMTSample.json
3. Get distinct "type" from Json file to use in later code
4. For each type, parse the data from "Clean Data" and store in google cloud storage
5. Create load job to load data from NDJson files named under "type" into jgr.dev tables in Big Query

Notes:
  - Leading number in orignal JSON could be a time stamp, added leading number into proceding json object
