import { invoke } from "@tauri-apps/api/core";

export async function call<T>(command: string, args?: Record<string, unknown>): Promise<T> {
  return invoke<T>(command, args);
}

export function isTauri(): boolean {
  return "__TAURI_INTERNALS__" in window;
}
