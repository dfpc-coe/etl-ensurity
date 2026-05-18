<h1 align='center'>ETL-Ensurity</h1>

<p align='center'>Pull vehicle data from Ensurity Mobile into the ETL platform.</p>

This ETL is intended to receive webhook payloads from [Ensurity Mobile](https://ensuritymobile.com/) so we can inspect the incoming vehicle data and shape the downstream transform. For now, the webhook accepts any payload and writes the body to the logs unchanged.

For local development, install dependencies with `npm install` and add a `.env` file with the ETL server connection values. In deployed environments, `ETL_API` and `ETL_LAYER` are provided by Lambda.

```json
{
    "ETL_API": "http://localhost:5001",
    "ETL_LAYER": "19",
    "APIKey": "replace-with-webhook-key"
}
```

Run the task with `ts-node task.ts`, or build first and run the compiled output with `npm run build` followed by `node dist/task.js`.

Deployment follows the same release flow as the other DFPC ETLs: GitHub Actions builds the container image on version tags, and CloudTAK handles configuration in AWS.

