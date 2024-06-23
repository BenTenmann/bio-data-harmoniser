import MappingTable from "@/components/mapping_table";

export default async function Mapping({ params }: { params: { id: string } }) {
  return (
    <MappingTable
      // mapping={mapping}
      runId={params.id}
      // ontology={ontology}
    />
  );
}
