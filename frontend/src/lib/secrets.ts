"use server";
import { endpoints } from "./endpoints";

export async function fetchApiKey(): Promise<string | undefined> {
  const response = await fetch(
    `${endpoints.secrets}/llm?hide_api_key=true`,
  );
  if (response.status === 404) {
    return;
  }
  const { llm_api_key } = await response.json();
  return llm_api_key;
}

export async function sendLlmApiKey(apiKey: string) {
  const response = await fetch(`${endpoints.secrets}/llm`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        llm_api_key: apiKey,
      }),
    });
    if (!response.ok) {
      throw new Error(response.statusText);
    }
}
