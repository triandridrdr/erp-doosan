export const getAttachedBomMasterId = (payload: unknown): number | undefined => {
  const p: any = payload as any;
  const raw = p?.system?.bomMasterId;
  if (raw === null || raw === undefined) return undefined;
  if (typeof raw === 'number') return Number.isFinite(raw) ? raw : undefined;
  const s = String(raw).trim();
  if (!s) return undefined;
  const n = Number(s);
  return Number.isFinite(n) ? n : undefined;
};
