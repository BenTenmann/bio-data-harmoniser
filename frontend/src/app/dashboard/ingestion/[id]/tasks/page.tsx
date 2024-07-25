import { DAG } from "@/components/dag";
import "reactflow/dist/style.css";
import "tailwindcss/tailwind.css";
import { endpoints } from "@/lib/endpoints";

export const dynamic = "force-dynamic";

async function getDag(runId: string) {
  const response = await fetch(`${endpoints.dag}/${runId}`);
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
  return <DAG dag={dag} runId={params.id} />;
}
