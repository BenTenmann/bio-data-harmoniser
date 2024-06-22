import { DAG } from "@/components/dag";
import IngestionDashboard from "@/components/ingestion_dashboard";
import { getDagRun } from "@/lib/utils";
import "reactflow/dist/style.css";
import "tailwindcss/tailwind.css";

export const dynamic = "force-dynamic";

async function getDag(runId: string) {
  const response = await fetch(
      `http://0.0.0.0:80/dag/${runId}`,
  )
  if (!response.ok) {
    throw new Error("Failed to fetch DAG");
  }
  return response.json();
}

export default async function TasksPage({
  params,
}: {
  params: { id: string };
}) {
  const dag = await getDag(params.id);
  return (
      <DAG dag={dag} runId={params.id} />
  );
}
