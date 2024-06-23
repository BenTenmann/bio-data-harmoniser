import { Table, TableBody, TableCell, TableRow } from "@/components/table";
import { ChevronDoubleRightIcon } from "@heroicons/react/24/outline";
import { SqlQuery } from "@/components/sql_query";

export const dynamic = "force-dynamic";

function Catalog({ catalog }: { catalog: any[] }) {
  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-row items-center gap-2">
        <ChevronDoubleRightIcon className="h-4 w-4" />
        <div className="text-xl font-medium">Catalog</div>
      </div>
      <Table>
        <TableBody>
          {catalog.map((row) => (
            <TableRow key={row.name} href="#">
              <TableCell>{row.name}</TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}

async function getCatalog(id: string) {
  const res = await fetch(`http://0.0.0.0:80/catalog/${id}`);
  return await res.json();
}

export default async function SqlPage({ params }: { params: { id: string } }) {
  const catalog = await getCatalog(params.id);
  return (
    <div className="flex flex-row">
      <div className="w-3/4">
        <SqlQuery runId={params.id} />
      </div>
      <div className="w-1/4 border-l border-zinc-950/10 pl-2 pt-2">
        <Catalog catalog={catalog} />
      </div>
    </div>
  );
}
