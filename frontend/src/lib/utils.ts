const statusColors = {
  running: "cyan",
  success: "lime",
  failed: "rose",
};

export { statusColors };

export async function getDagRun(id: string) {
  const res = await fetch(
    "http://localhost:8080/api/v1/dags/data_extraction/dagRuns/" + id,
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
