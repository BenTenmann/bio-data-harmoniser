import IngestionDashboard from "@/components/ingestion_dashboard";
import { getDagRun } from "@/lib/server_funcs";

export default async function IngestionLayout({
  children,
  params,
}: {
  children: React.ReactNode;
  params: { id: string };
}) {
  const datum = await getDagRun(params.id);
  return <IngestionDashboard datum={datum}>{children}</IngestionDashboard>;
}
