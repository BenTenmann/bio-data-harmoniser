"use server";
import { endpoints } from "./endpoints";

import { QueryStatus } from "@/lib/utils";

export async function submitSql(query: string, runId: string): Promise<string> {
  const response = await fetch(
    endpoints.sqlQueryDagRuns,
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
    `${endpoints.sqlQueryDagRuns}/${query_id}`,
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
  const response = await fetch(`${endpoints.sql}/${queryId}`);
  return await response.json();
}
