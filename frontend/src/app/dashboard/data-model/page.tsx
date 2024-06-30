import { Button } from "@/components/button";
import { ChevronRightIcon, PlusIcon } from "@heroicons/react/20/solid";
import React from "react";

export const dynamic = "force-dynamic";

async function getSchemas() {
  const res = await fetch("http://0.0.0.0:80/schemas");
  return await res.json();
}

export default async function SchemaDashboard() {
  const schemas = await getSchemas();
  return (
    <>
      <div>
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-semibold leading-6 text-gray-900">
            Schemas
          </h1>
          <Button href="/dashboard/data-model/create">
            <PlusIcon className="text-white" /> Create
          </Button>
        </div>
        <ul role="list" className="divide-y divide-gray-100">
          {schemas.map(
            (schema: { name: string; description: string; columns: any }) => (
              <li
                key={schema.name}
                className="relative flex justify-between gap-x-6 px-4 py-5 hover:bg-gray-50 sm:px-6 lg:px-8"
              >
                <div className="flex min-w-0 gap-x-4">
                  <div className="min-w-0 flex-auto">
                    <p className="text-sm font-semibold leading-6 text-gray-900">
                      <a>
                        <span className="absolute inset-x-0 -top-px bottom-0" />
                        {schema.name}
                      </a>
                    </p>
                    <p className="mt-1 flex text-xs leading-5 text-gray-500">
                      <a className="relative truncate">{schema.description}</a>
                    </p>
                  </div>
                </div>
                <div className="flex shrink-0 items-center gap-x-4">
                  <div className="hidden sm:flex sm:flex-col sm:items-end">
                    <p className="text-sm leading-6 text-gray-900">
                      {Object.keys(schema.columns).length} columns
                    </p>
                  </div>
                  <ChevronRightIcon
                    className="h-5 w-5 flex-none text-gray-400"
                    aria-hidden="true"
                  />
                </div>
              </li>
            ),
          )}
        </ul>
      </div>
    </>
  );
}
