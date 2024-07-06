"use server";

export async function fetchApiKey(): Promise<string | undefined> {
  const response = await fetch(
    `http://0.0.0.0:80/secrets/llm?hide_api_key=true`,
  );
  if (response.status === 404) {
    return;
  }
  const { llm_api_key } = await response.json();
  return llm_api_key;
}
