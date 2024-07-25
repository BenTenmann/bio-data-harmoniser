import { ChevronRightIcon, PlusIcon } from "@heroicons/react/20/solid";
import { ExclamationCircleIcon } from "@heroicons/react/24/outline";
import { Button } from "@/components/button";
import { statusColors } from "@/lib/utils";
import React from "react";
import { Text } from "@/components/text";
import { clsx } from "clsx";
import { endpoints } from "@/lib/endpoints";

export const dynamic = "force-dynamic";

type IngestionParams = {
  user_id: string;
  name: string;
  description: string;
  url: string;
};

type Ingestion = {
  id: string;
  params: IngestionParams;
  status: keyof typeof statusColors;
  execution_date: number;
};

const colours = {
  running: ["bg-cyan-500/20", "bg-cyan-500"],
  success: ["bg-lime-500/20", "bg-lime-500"],
  failed: ["bg-rose-500/20", "bg-rose-500"],
};

async function getIngestions(): Promise<Ingestion[]> {
  const userId = "test_user";
  const response = await fetch(`${endpoints.ingestions}/${userId}`);
  return response.json();
}

function StatusComponent({ status }: { status: keyof typeof statusColors }) {
  const [backgroundColour, foregroundColour] = colours[status];
  return (
    <div className="mt-1 flex items-center gap-x-1.5">
      <div
        className={clsx("flex-none", "rounded-full", backgroundColour, "p-1")}
      >
        <div
          className={clsx("h-1.5", "w-1.5", "rounded-full", foregroundColour)}
        />
      </div>
      <p className="text-xs leading-5 text-gray-500">{status}</p>
    </div>
  );
}

function IngestionPage({ ingestions }: { ingestions: Ingestion[] }) {
  return (
    <div>
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold leading-6 text-gray-900">
          Ingestion
        </h1>
        <Button href="/dashboard/ingestion/create">
          <PlusIcon className="text-white" /> Create
        </Button>
      </div>
      {ingestions.length > 0 ? (
        <ul role="list" className="divide-y divide-gray-100">
          {ingestions.map((ingestion) => (
            <li
              key={ingestion.id}
              className="relative flex justify-between gap-x-6 px-4 py-5 hover:bg-gray-50 sm:px-6 lg:px-8"
            >
              <div className="flex min-w-0 gap-x-4">
                {/*<img className="h-12 w-12 flex-none rounded-full bg-gray-50" src={person.imageUrl} alt=""/>*/}
                <div className="min-w-0 flex-auto">
                  <p className="text-sm font-semibold leading-6 text-gray-900">
                    <a href={`/dashboard/ingestion/${ingestion.id}/tasks`}>
                      <span className="absolute inset-x-0 -top-px bottom-0" />
                      {ingestion.params.name}
                    </a>
                  </p>
                  <p className="mt-1 flex text-xs leading-5 text-gray-500">
                    <a className="relative truncate">
                      {ingestion.params.description}
                    </a>
                  </p>
                </div>
              </div>
              <div className="flex shrink-0 items-center gap-x-4">
                <div className="hidden sm:flex sm:flex-col sm:items-end">
                  <p className="text-sm leading-6 text-gray-900">
                    {new Date(ingestion.execution_date).toDateString()}
                  </p>
                  <StatusComponent status={ingestion.status} />
                </div>
                <ChevronRightIcon
                  className="h-5 w-5 flex-none text-gray-400"
                  aria-hidden="true"
                />
              </div>
            </li>
          ))}
        </ul>
      ) : (
        <div className="flex h-64 flex-row items-center justify-center gap-x-2">
          <ExclamationCircleIcon className="h-6 w-6 text-gray-400" />
          <Text className="text-gray-500">No ingestions found</Text>
        </div>
      )}
    </div>
  );
}

export default async function Home() {
  const ingestions = await getIngestions();
  return <IngestionPage ingestions={ingestions} />;
}
