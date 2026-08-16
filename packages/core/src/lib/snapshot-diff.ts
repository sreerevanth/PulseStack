import type { SnapshotDiff } from '@pulsestack/contracts';

type JsonRecord = Record<string, unknown>;

export function diffSnapshotState(
  before: JsonRecord | undefined,
  after: JsonRecord | undefined,
): SnapshotDiff {
  const diff: SnapshotDiff = { added: [], modified: [], removed: [] };
  collectDiff(before ?? {}, after ?? {}, '', diff);
  return diff;
}

function collectDiff(
  before: unknown,
  after: unknown,
  path: string,
  diff: SnapshotDiff,
) {
  if (isPlainObject(before) && isPlainObject(after)) {
    const keys = Array.from(
      new Set([...Object.keys(before), ...Object.keys(after)]),
    ).sort((a, b) => a - b);
    for (const key of keys) {
      const nextPath = path ? `${path}.${key}` : key;
      const hasBefore = Object.prototype.hasOwnProperty.call(before, key);
      const hasAfter = Object.prototype.hasOwnProperty.call(after, key);
      if (!hasBefore) {
        diff.added.push({ path: nextPath, after: after[key] });
      } else if (!hasAfter) {
        diff.removed.push({ path: nextPath, before: before[key] });
      } else {
        collectDiff(before[key], after[key], nextPath, diff);
      }
    }
    return;
  }

  if (!sameValue(before, after)) {
    diff.modified.push({ path, before, after });
  }
}

function isPlainObject(value: unknown): value is JsonRecord {
  return Boolean(value) && typeof value === 'object' && !Array.isArray(value);
}

function sameValue(left: unknown, right: unknown) {
  return stableStringify(left) === stableStringify(right);
}

function stableStringify(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(stableStringify).join(',')}]`;
  }
  if (isPlainObject(value)) {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableStringify(value[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}
