import { existsSync, readFileSync, renameSync, writeFileSync } from 'node:fs';

// 先写临时文件再 rename，保证 state.json 不会写出半个文件。
export function saveStateFile(path, state) {
  const tmp = `${path}.tmp`;
  writeFileSync(tmp, JSON.stringify(state, null, 2));
  renameSync(tmp, path);
}

export function loadStateFile(path) {
  if (!existsSync(path)) return null;
  return JSON.parse(readFileSync(path, 'utf8'));
}
