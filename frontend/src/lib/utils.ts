const statusColors = {
  running: "cyan",
  success: "lime",
  failed: "rose",
};

export { statusColors };

export enum Dtype {
  str = "str",
  int = "int",
  float = "float",
  list_str = "list[str]",
  Ancestry = "Ancestry",
  Sex = "Sex",
  GenomeBuild = "GenomeBuild",
  Species = "Species",
  Tissue = "Tissue",
  Trait = "Trait",
  AminoAcidSequence = "AminoAcidSequence",
}

export function getDtypeNames() {
  return Object.keys(Dtype).filter((key) => Dtype[key]);
}

export enum QueryStatus {
  RUNNING = "running",
  SUCCESS = "success",
  FAILED = "failed",
}

export type Field = {
  name: string;
  dtype: Dtype;
  aliases: string[];
  required: boolean;
  default: any;
  description: string;
};

export type Schema = {
  name: string;
  description: string;
  fields: Field[];
};

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
