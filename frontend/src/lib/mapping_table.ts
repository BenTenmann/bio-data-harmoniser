"use server";

export type Mapping = {
  mention: string;
  id: string;
  name: string;
  normalised_score: number;
  types: string[];
  mapping_id: number;
};
export type Entity = {
  id: string;
  name: string;
  iri: string;
};

export async function fetchMappings(run_id: string) {
  const response = await fetch(`http://0.0.0.0:80/mappings/${run_id}`);
  return await response.json();
}

export async function updateMapping(
  run_id: string,
  mapping: Mapping,
  entity: Entity,
): Promise<boolean> {
  const response = await fetch(`http://0.0.0.0:80/mappings/${run_id}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      mapping: mapping,
      entity: entity,
    }),
  });
  if (response.ok) {
    console.log("Updated mapping");
    return true;
  } else {
    console.log("Failed to update mapping");
    return false;
  }
}

export async function fetchEntities(
  types: string[],
  query?: string,
  limit?: number,
) {
  const response = await fetch(`http://0.0.0.0:80/entities`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      types: types,
      query: query,
      limit: limit,
    }),
  });
  return await response.json();
}
