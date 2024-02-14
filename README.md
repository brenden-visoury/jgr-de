JGR DE Assessment

Workflow:

1. Read data from SMTSample.json
2. "Clean" data from SMTSample.json
3. Get distinct "type" from Json file to use in later code
4. For each type, parse the data from "Clean Data" and store in google cloud storage
5. Create load job to load data from NDJson files named under "type" into jgr.dev tables in Big Query

Notes:
  - Leading number in orignal JSON could be a time stamp, added leading number into proceding json object

Results:

json files for each "type"

![Alt text](https://private-user-images.githubusercontent.com/58221549/304662477-ea7d965a-1c65-404a-adbd-b42665dcf00b.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MDc4OTc4OTIsIm5iZiI6MTcwNzg5NzU5MiwicGF0aCI6Ii81ODIyMTU0OS8zMDQ2NjI0NzctZWE3ZDk2NWEtMWM2NS00MDRhLWFkYmQtYjQyNjY1ZGNmMDBiLnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNDAyMTQlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjQwMjE0VDA3NTk1MlomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPWMxODczMzA1OTA2YzQ4ZjdmOGEwMmUyMDkzOGI1MzQzOTVjYzgwMzVmZDI5ZmI2Y2YwOWU5YzRhZjc2NzAwODgmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0JmFjdG9yX2lkPTAma2V5X2lkPTAmcmVwb19pZD0wIn0.3EPqM-5anGnVwWJh9yEzI0FK1Ph3IeMjZoewsr4Fam4)
tables in big query
![Alt text](https://private-user-images.githubusercontent.com/58221549/304662475-99a6ea9e-cc47-46f8-8897-bb44b8342ec9.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MDc4OTc4OTIsIm5iZiI6MTcwNzg5NzU5MiwicGF0aCI6Ii81ODIyMTU0OS8zMDQ2NjI0NzUtOTlhNmVhOWUtY2M0Ny00NmY4LTg4OTctYmI0NGI4MzQyZWM5LnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNDAyMTQlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjQwMjE0VDA3NTk1MlomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPTdiODIwMjYyOTBiMmE0OGMzOTIyYWYwNGRiMTZjZmJmYjJkZjEzNmUzNTk5Nzg3MTRiMDg0NjYzNTMxODY1ZGYmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0JmFjdG9yX2lkPTAma2V5X2lkPTAmcmVwb19pZD0wIn0.wOGKo4M0jEnNz0VddusQzvpbLZ3GohRcXE-wxyrUMe8)
'linecrossing' table in big query
![Alt text](https://private-user-images.githubusercontent.com/58221549/304662478-a973f3b7-9fa0-421c-9aec-7b96e0207878.png?jwt=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJnaXRodWIuY29tIiwiYXVkIjoicmF3LmdpdGh1YnVzZXJjb250ZW50LmNvbSIsImtleSI6ImtleTUiLCJleHAiOjE3MDc4OTc4OTIsIm5iZiI6MTcwNzg5NzU5MiwicGF0aCI6Ii81ODIyMTU0OS8zMDQ2NjI0NzgtYTk3M2YzYjctOWZhMC00MjFjLTlhZWMtN2I5NmUwMjA3ODc4LnBuZz9YLUFtei1BbGdvcml0aG09QVdTNC1ITUFDLVNIQTI1NiZYLUFtei1DcmVkZW50aWFsPUFLSUFWQ09EWUxTQTUzUFFLNFpBJTJGMjAyNDAyMTQlMkZ1cy1lYXN0LTElMkZzMyUyRmF3czRfcmVxdWVzdCZYLUFtei1EYXRlPTIwMjQwMjE0VDA3NTk1MlomWC1BbXotRXhwaXJlcz0zMDAmWC1BbXotU2lnbmF0dXJlPWRkZTZlMzAwZjgzZTNhMGZhNGVmNTI1ZTY0NmQ4MGVmZmU4ZjhmZDE0OTYwYzY4OThlYjE5NjQ4ZDJiMmQ5ZmQmWC1BbXotU2lnbmVkSGVhZGVycz1ob3N0JmFjdG9yX2lkPTAma2V5X2lkPTAmcmVwb19pZD0wIn0.3S3j1baWXVQVgOoQVhtaauLaeL065PRiaOik1pj4PCI)
