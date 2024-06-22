"use client";
import { ArrowTopRightOnSquareIcon } from "@heroicons/react/20/solid";
import { Button } from "@/components/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/table";
import { Select } from "@/components/select";
import { Input } from "@/components/input";
import { Listbox, ListboxOption, ListboxLabel } from "@/components/listbox";
import {
  Dropdown,
  DropdownButton,
  DropdownItem,
  DropdownMenu,
} from "@/components/dropdown";
import { Badge, colors as badgeColors } from "@/components/badge";
import {
  Pagination,
  PaginationGap,
  PaginationList,
  PaginationNext,
  PaginationPage,
  PaginationPrevious,
} from "@/components/pagination";
import React from "react";
import { useSearchParams } from "next/navigation";
import debounce from "lodash.debounce";
import { MenuButton as HeadlessMenuButton } from "@headlessui/react";
import {
  type Entity,
  type Mapping,
  fetchMappings,
  updateMapping,
  fetchEntities,
} from "@/lib/mapping_table";

type ConfidenceValue =
  | 0
  | 5
  | 10
  | 15
  | 20
  | 25
  | 30
  | 35
  | 40
  | 45
  | 50
  | 55
  | 60
  | 65
  | 70
  | 75
  | 80
  | 85
  | 90
  | 95
  | 100;

const valueToColorMap: Record<ConfidenceValue, keyof typeof badgeColors> = {
  0: "red",
  5: "red",
  10: "red",
  15: "orange",
  20: "orange",
  25: "orange",
  30: "amber",
  35: "amber",
  40: "amber",
  45: "yellow",
  50: "yellow",
  55: "yellow",
  60: "yellow",
  65: "lime",
  70: "lime",
  75: "lime",
  80: "green",
  85: "green",
  90: "green",
  95: "green",
  100: "green",
};

function getOpacityValue({
  normalisedScore,
}: {
  normalisedScore: number;
}): keyof typeof badgeColors {
  const score = 100 * normalisedScore;
  const opacityValues = [
    0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90,
    95, 100,
  ];
  const diff = opacityValues.map((value) => Math.abs(value - score));
  const minDiff = Math.min(...diff);
  const index = diff.indexOf(minDiff);
  return valueToColorMap[opacityValues[index] as ConfidenceValue];
}

function exportCSV({ mapping }) {
  const csv = mapping
    .map(
      (item) =>
        `${item.mention},${item.id},${item.name},${item.normalised_score}`,
    )
    .join("\n");
  const blob = new Blob(["mention,id,name,score\n" + csv], {
    type: "text/csv",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "mapping.csv";
  a.click();
  URL.revokeObjectURL(url);
}

export default function MappingTable({ runId }: { runId: string }) {
  const [mapping, setMapping] = React.useState<Mapping[]>([]);

  const searchParams = useSearchParams();
  const page = Number(searchParams.get("page") || 1);
  const pageSize = 5;
  const nPages = Math.ceil(mapping.length / pageSize);
  const pageWindow = [page];
  if (page > 1) {
    pageWindow.unshift(page - 1);
  }
  if (page < nPages) {
    pageWindow.push(page + 1);
  }
  const [entities, setEntities] = React.useState<Entity[]>([]);
  const [focusIdx, setFocusIdx] = React.useState<number | null>(null);
  const [search, setSearch] = React.useState<string | null>(null);

  React.useEffect(() => {
    console.log("Fetching mappings");
    const _fetchMapping = async () => {
      const data = await fetchMappings(runId);
      setMapping(data);
    };
    _fetchMapping();
  }, [runId]);

  const handleFocus = (idx: number | null) => {
    console.log("Focused");
    if (idx === null) {
      setEntities([]);
    }
    setFocusIdx(idx);
    setSearch("");
  };

  async function getEntities(types: string[], query?: string) {
    if (!query) {
      console.log("No query");
      setEntities([]);
      return;
    }
    const data = await fetchEntities(types, query, 10);
    setEntities(data);
  }

  const getEntitiesDebounced = debounce(getEntities, 300);

  const handleBlur = () => {
    console.log("Blurred");
    setFocusIdx(null);
    setSearch(null);
    setEntities([]);
  };

  const handleClick = async (entity: Entity, mapping: Mapping) => {
    console.log("Clicked");
    const success = await updateMapping(runId, mapping, entity);
    if (success) {
      const data = await fetchMappings(runId);
      setMapping(data);
      handleBlur();
    } else {
      alert("Failed to update mapping");
    }
  };
  return (
    <>
      <div className="flex items-center justify-between">
        <h1 className="text-2xl font-semibold leading-6 text-gray-900"></h1>
        <div className="mt-4 flex items-center justify-between">
          <div>
            <Button color="light" onClick={() => exportCSV({ mapping })}>
              <ArrowTopRightOnSquareIcon aria-hidden="true" /> Export
            </Button>
          </div>
        </div>
      </div>
      <Table>
        <TableHead>
          <TableRow>
            <TableHeader>Mention</TableHeader>
            <TableHeader>Entity</TableHeader>
            <TableHeader>Confidence</TableHeader>
          </TableRow>
        </TableHead>
        <TableBody>
          {mapping
            .slice((page - 1) * pageSize, page * pageSize)
            .map((item, index) => (
              <TableRow key={index}>
                <TableCell>
                  <div className="flex items-center gap-4">
                    <div>
                      <div className="font-medium">{item.mention}</div>
                      <div className="text-xs text-zinc-400">
                        {item.types.join(", ")}
                      </div>
                    </div>
                  </div>
                </TableCell>
                <TableCell>
                  <Dropdown>
                    <HeadlessMenuButton
                      as={Input}
                      value={search !== null ? search : item.name}
                      onFocus={(e) => {
                        handleFocus(index);
                      }}
                      onBlur={(e) => entities.length === 0 && handleBlur()}
                      onChange={(e) => {
                        setSearch(e.target.value);
                        getEntitiesDebounced(item.types, e.target.value);
                      }}
                    />
                    {index === focusIdx && (
                      <DropdownMenu show={true} className="min-w-96">
                        {entities.length > 0 ? (
                          entities.map((entity) => (
                            <DropdownItem
                              key={entity.id}
                              onClick={() => handleClick(entity, item)}
                            >
                              {entity.name}
                            </DropdownItem>
                          ))
                        ) : (
                          <DropdownItem disabled>
                            No entities found
                          </DropdownItem>
                        )}
                      </DropdownMenu>
                    )}
                  </Dropdown>
                </TableCell>
                <TableCell>
                  <Badge
                    color={getOpacityValue({
                      normalisedScore: item.normalised_score,
                    })}
                  >
                    {item.normalised_score.toFixed(2)}
                  </Badge>
                </TableCell>
              </TableRow>
            ))}
        </TableBody>
      </Table>
      <Pagination className="mt-6">
        <PaginationPrevious href={page > 1 ? `?page=${page - 1}` : undefined} />
        <PaginationList>
          {Array.from({ length: Math.min(Math.max(0, page - 2), 2) }).map(
            (_, index) => (
              <PaginationPage key={index} href={`?page=${index + 1}`}>
                {String(index + 1)}
              </PaginationPage>
            ),
          )}
          {page > 3 ? <PaginationGap /> : ""}
          {pageWindow.map((index) => (
            <PaginationPage
              key={index}
              href={`?page=${index}`}
              current={index === page}
            >
              {String(index)}
            </PaginationPage>
          ))}
          {nPages - page > 3 ? <PaginationGap /> : ""}
          {Array.from({ length: Math.min(nPages - page, 2) }).map(
            (_, index) => (
              <PaginationPage
                key={nPages - 1 + index}
                href={`?page=${nPages - 1 + index}`}
              >
                {String(nPages - 1 + index)}
              </PaginationPage>
            ),
          )}
        </PaginationList>
        <PaginationNext
          href={page !== nPages ? `?page=${page + 1}` : undefined}
        />
      </Pagination>
    </>
  );
}
