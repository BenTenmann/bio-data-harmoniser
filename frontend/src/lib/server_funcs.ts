"use server";
import { endpoints } from "@/lib/endpoints";
import { QueryStatus } from "@/lib/utils";

type Query = {
  id: string;
  name: string;
  description: string;
  status: QueryStatus;
  execution_date: number;
  conf: any;
};

export async function getDagRun(id: string): Promise<Query> {
  const res = await fetch(
    `${endpoints.dataExtractionDagRuns}/${id}`,
    {
      headers: {
        Authorization: "Basic " + btoa("admin:admin"),
      },
      method: "GET",
    },
  );
  const jsonData = await res.json();
  return {
    id: jsonData.dag_run_id,
    name: jsonData.conf.name,
    description: jsonData.conf.description,
    status: jsonData.state,
    execution_date: Date.parse(jsonData.execution_date),
    conf: jsonData.conf,
  };
}

export async function submitDagRun(conf: any): Promise<{ resOk: boolean, runId?: string }> {
  const res = await fetch(
      endpoints.dataExtractionDagRuns,
      {
        headers: {
          Authorization: "Basic " + btoa("admin:admin"),
          "Content-Type": "application/json",
        },
        method: "POST",
        body: JSON.stringify({
          conf: conf,
        }),
      },
    );
    const out: { resOk: boolean, runId?: string } = { resOk: res.ok };
    if (res.ok) {
      const jsonData = await res.json();
      out.runId = jsonData.dag_run_id;
    }
    return out;
}

export async function uploadFile(fileFormData: FormData): Promise<string> {
  const response = await fetch(endpoints.ingestionFileUpload, {
      method: "POST",
      body: fileFormData,
    });
  if (!response.ok) {
    throw new Error(response.statusText);
  }
  const jsonData: { url: string } = await response.json();
  return jsonData.url;
}

export async function getPaperIngestionMetadata(url: string): Promise<{ name: string, description: string } | undefined> {
  console.log(endpoints.paperIngestionMetadata);
  const res = await fetch(endpoints.paperIngestionMetadata, {
      headers: {
        "Content-Type": "application/json",
      },
      method: "POST",
      body: JSON.stringify({
        url: url,
      }),
    });
  if (res.status === 200) {
    const jsonData = await res.json();
    return {
      name: jsonData.name,
      description: jsonData.description,
    };
  }
  return;
}
