"use server";

import { QueryStatus } from "@/lib/utils";

export async function submitSql(query: string, runId: string): Promise<string> {
  const response = await fetch(
    `http://localhost:8080/api/v1/dags/sql_query/dagRuns`,
    {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: "Basic " + btoa("admin:admin"),
      },
      body: JSON.stringify({
        conf: {
          sql_query: query,
          data_extraction_run_id: decodeURIComponent(runId),
        },
      }),
    },
  );
  const jsonData = await response.json();
  return jsonData.dag_run_id;
}

export async function getQueryStatus(query_id: string): Promise<QueryStatus> {
  const response = await fetch(
    `http://localhost:8080/api/v1/dags/sql_query/dagRuns/${query_id}`,
    {
      headers: {
        "Content-Type": "application/json",
        Authorization: "Basic " + btoa("admin:admin"),
      },
    },
  );
  const jsonData = await response.json();
  if (jsonData.state === "failed") {
    return QueryStatus.FAILED;
  } else if (jsonData.state === "success") {
    return QueryStatus.SUCCESS;
  }
  return QueryStatus.RUNNING;
}

export async function getSqlResults(queryId: string) {
  const response = await fetch(`http://0.0.0.0:80/sql/${queryId}`);
  return await response.json();
}
