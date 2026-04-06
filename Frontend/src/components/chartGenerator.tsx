import { ChartData } from "@/config/api";

/** Normalize incoming rawData */
export function normalizeRows(rawData: unknown[]): Record<string, unknown>[] {
  if (!Array.isArray(rawData)) return [];

  // Already array of objects
  if (typeof rawData[0] === "object" && !Array.isArray(rawData[0])) {
    return rawData as Record<string, unknown>[];
  }

  // Format: [ ["col1","col2"], [ [1,2], [3,4] ] ]
  if (
    Array.isArray(rawData[0]) &&
    Array.isArray(rawData[1]) &&
    Array.isArray(rawData[1][0])
  ) {
    const columns = rawData[0] as string[];
    const rows = rawData[1] as unknown[][];

    return rows.map((row) =>
      Object.fromEntries(columns.map((col, idx) => [col, row[idx]]))
    );
  }

  return [];
}

/** Detect numeric and non-numeric columns */
function getNumericColumns(data: Record<string, unknown>[]) {
  if (!data.length) return { xKey: null, yKey: null };

  const sample = data[0];
  const keys = Object.keys(sample);

  const numericKeys = keys.filter((k) => {
    const v = sample[k];
    return typeof v === "number" || !isNaN(Number(v));
  });

  const nonNumericKeys = keys.filter((k) => !numericKeys.includes(k));

  return {
    xKey: nonNumericKeys[0] || keys[0], // typical X axis (date/string)
    yKey: numericKeys[0] || null,       // first numeric field
  };
}

/** Main chart generator */
export function generateChartsFromData(
  rawData: unknown[],
  chartTypes: { [key: number]: ChartData["type"] },
  // response: unknown,
  resultIndex: number,
  axisSelections: { x?: string; y?: string } = {}
): ChartData[] {
  const rows = normalizeRows(rawData);

  if (!rows.length) return [];

  const { xKey, yKey } = getNumericColumns(rows);

  const finalX = axisSelections.x || xKey;
  const finalY = axisSelections.y || yKey;

  if (!finalX || !finalY) return [];

  const chartType: ChartData["type"] =
    chartTypes[resultIndex] || "line";

  return [
    {
      type: chartType,
      title: `Chart for ${finalY} vs ${finalX}`,
      data: {
        labels: rows.map((row) => String(row[finalX])),
        datasets: [
          {
            label: finalY,
            data: rows.map((row) => Number(row[finalY])),
          },
        ],
      },
    },
  ];
}
