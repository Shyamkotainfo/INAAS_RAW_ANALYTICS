
'use client';

import React, { useState, useMemo } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  RadialLinearScale,
  Title,
  Tooltip,
  Legend,
  Filler,

} from 'chart.js';
import {
  Line,
  Bar,
  Pie,
  Doughnut,
  Radar,
  PolarArea,
  Bubble,
  Scatter,
} from 'react-chartjs-2';
import { ChartData } from '../config/api';
import type { Plugin, Element, } from 'chart.js'; // ⬅️ Added ChartJSData and ChartOptions for clarity

// Register core Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  RadialLinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
  Filler
);


// Helper to safely get element position (for label placement)
function getElementPosition(element: unknown): { x: number; y: number } | null {
  const maybe = element as {
    tooltipPosition?: (useFinalPosition?: boolean) => { x: number; y: number };
    x?: number;
    y?: number;
  };

  // ✅ Use tooltipPosition() if available
  if (typeof maybe?.tooltipPosition === 'function') {
    const pos = maybe.tooltipPosition(true);
    if (pos && typeof pos.x === 'number' && typeof pos.y === 'number') {
      return pos;
    }
  }

  // ✅ Fallback to direct coordinates
  if (typeof maybe?.x === 'number' && typeof maybe?.y === 'number') {
    return { x: maybe.x, y: maybe.y };
  }

  return null;
}

// Type guard for chart data objects
function isChartDataObject(value: unknown): value is { y?: number; r?: number } {
  return typeof value === 'object' && value !== null && ('y' in value || 'r' in value);
}

const ValueLabelsPlugin: Plugin<
  'bar' | 'line' | 'pie' | 'doughnut' | 'radar' | 'polarArea' | 'bubble' | 'scatter'
> = {
  id: 'valueLabels',
  afterDatasetsDraw: (chart, _args, options) => {
    const opts = options as Record<string, unknown>;
    const showLabels = Boolean(opts['showLabels']);
    if (!showLabels) return;

    const { ctx, data } = chart;
    ctx.save();
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillStyle = '#2c5aa0';
    ctx.font = '12px sans-serif';

    const drawnPositions: { x: number; y: number }[] = [];

    data.datasets.forEach((dataset, datasetIndex) => {
      const meta = chart.getDatasetMeta(datasetIndex);
      if (meta.hidden) return;

      meta.data.forEach((element, index) => {
        const rawValue = dataset.data[index];
        const value = isChartDataObject(rawValue)
          ? rawValue.y ?? rawValue.r ?? ''
          : rawValue;

        // ✅ Handle dataset that requests label text instead of numeric value
        const dsAny = dataset as unknown as { dataLabelFromLabel?: boolean };
        let displayText: string | null = null;
        if (dsAny?.dataLabelFromLabel === true && Array.isArray(data.labels)) {
          const label = (data.labels as unknown[])[index];
          if (label != null) displayText = String(label);
        }
        if (displayText === null) {
          if (value == null || (typeof value === 'string' && value === '')) return;
          displayText = typeof value === 'number' ? String(value) : `${value}`;
        }


        const pos = getElementPosition(element as unknown as Element);
        if (!pos || Number.isNaN(pos.x) || Number.isNaN(pos.y)) return;

        // ✅ Chart-type specific Y-offsets
        const x = pos.x;
        let y = pos.y;
        if (meta.type === 'bar') {
          y -= 8;
        } else if (
          meta.type === 'line' ||
          meta.type === 'scatter' ||
          meta.type === 'bubble'
        ) {
          y -= 8;
        } else if (
          meta.type === 'pie' ||
          meta.type === 'doughnut' ||
          meta.type === 'polarArea'
        ) {
          // Draw inside arc — no offset needed
        }

        // ✅ Prevent overlapping labels - move up if needed, but avoid top area where toggle is
        const toggleAreaTop = 25; // Area to avoid near toggle button
        while (drawnPositions.some((p) => Math.abs(p.x - x) < 18 && Math.abs(p.y - y) < 12)) {
          if (y > toggleAreaTop) {
            y -= 10; // Move up if we're below toggle area (preferred)
          } else {
            y += 10; // Move down if we're in toggle area
          }
        }
        
        // ✅ Final check: if label is too close to toggle area, move it down slightly
        if (y < toggleAreaTop && y > 5) {
          y = toggleAreaTop + 3;
        }

        ctx.fillText(displayText, x, y);
        drawnPositions.push({ x, y });
      });
    });

    ctx.restore();
  },
};


// Register plugin once
ChartJS.register(ValueLabelsPlugin);

interface ChartRendererProps {
  chartData: ChartData;
}

const ChartRenderer: React.FC<ChartRendererProps> = ({ chartData }) => {
  const [showLabels, setShowLabels] = useState(true);

  const BASE_BG_COLORS = [
    'rgba(255, 99, 132, 0.2)',
    'rgba(255, 159, 64, 0.2)',
    'rgba(255, 205, 86, 0.2)',
    'rgba(75, 192, 192, 0.2)',
    'rgba(54, 162, 235, 0.2)',
    'rgba(153, 102, 255, 0.2)',
    'rgba(201, 203, 207, 0.2)',
  ];
  const BASE_BORDER_COLORS = [
    'rgb(255, 99, 132)',
    'rgb(255, 159, 64)',
    'rgb(255, 205, 86)',
    'rgb(75, 192, 192)',
    'rgb(54, 162, 235)',
    'rgb(153, 102, 255)',
    'rgb(201, 203, 207)',
  ];

  const makePalette = (base: string[], length: number) =>
    Array.from({ length }, (_, i) => base[i % base.length]);

  const incoming = chartData.data as Record<string, unknown>;
  const rawDatasets = Array.isArray(incoming.datasets)
    ? (incoming.datasets as Array<Record<string, unknown>>)
    : [];
  const labels = Array.isArray(incoming.labels)
    ? (incoming.labels as string[])
    : [];

  const safeData = {
    labels,
    datasets: rawDatasets.map((datasetObj) => {
      const dataArray = Array.isArray(datasetObj.data as unknown[])
        ? (datasetObj.data as unknown[])
        : [];
      const points = Math.max(dataArray.length, labels.length, BASE_BG_COLORS.length);
      const borderWidth =
        typeof datasetObj.borderWidth === 'number' ? datasetObj.borderWidth : 2;
      return {
        ...datasetObj,
        backgroundColor: makePalette(BASE_BG_COLORS, points),
        borderColor: makePalette(BASE_BORDER_COLORS, points),
        borderWidth,
      } as Record<string, unknown>;
    }),
  };

  // 👇 Important: plugin options reactively depend on showLabels
  const commonOptions = useMemo(
    () => ({
      responsive: true,
      maintainAspectRatio: false,
      layout: {
        padding: {
          top: 30, // Extra top padding to prevent value labels from being cut off
        },
      },
      plugins: {
        legend: { display: false },
        title: { display: false },
        tooltip: { enabled: true },
        valueLabels: { showLabels }, // ✅ pass toggle state into plugin options
      },
      interaction: { mode: 'nearest' as const, axis: 'x' as const, intersect: false },
      scales: {
        x: { grid: { color: 'rgba(44,90,160,0.1)' }, ticks: { color: '#666' } },
        y: { grid: { color: 'rgba(44,90,160,0.1)' }, ticks: { color: '#666' } },
      },
    }),
    [showLabels] // ✅ triggers re-render when toggled
  );

  const renderChart = () => {
    switch (chartData.type) {
      case 'line':
        return (
          <Line
            data={safeData as unknown as Parameters<typeof Line>[0]['data']}
            options={{
              ...commonOptions,
              ...(chartData.options as Record<string, unknown> || {}),
              elements: {
                line: { tension: 0.4 },
                point: { radius: 3, hoverRadius: 5 },
              },
            }}
          />
        );

      case 'bar':
        return (
          <Bar
            data={safeData as unknown as Parameters<typeof Bar>[0]['data']}
            options={{
              ...commonOptions,
              ...(chartData.options as Record<string, unknown> || {}),
            }}
          />
        );

      case 'pie':
        return (
          <Pie
            data={safeData as unknown as Parameters<typeof Pie>[0]['data']}
            options={{
              ...commonOptions,
              scales: undefined,
            }}
          />
        );

      case 'doughnut':
        return (
          <Doughnut
            data={safeData as unknown as Parameters<typeof Doughnut>[0]['data']}
            options={{
              ...commonOptions,
              scales: undefined,
            }}
          />
        );

      case 'radar':
        return (
          <Radar
            data={safeData as unknown as Parameters<typeof Radar>[0]['data']}
            options={{
              ...commonOptions,
              scales: {
                r: {
                  grid: { color: '#E5E7EB' },
                  ticks: { color: '#6B7280' },
                },
              },
            }}
          />
        );

      case 'polarArea':
        return (
          <PolarArea
            data={safeData as unknown as Parameters<typeof PolarArea>[0]['data']}
            options={{
              ...commonOptions,
              scales: undefined,
            }}
          />
        );

      case 'bubble':
        return (
          <Bubble
            data={safeData as unknown as Parameters<typeof Bubble>[0]['data']}
            options={{
              ...commonOptions,
              scales: {
                x: {
                  grid: { color: '#E5E7EB' },
                  ticks: { color: '#6B7280' },
                },
                y: {
                  grid: { color: '#E5E7EB' },
                  ticks: { color: '#6B7280' },
                },
              },
            }}
          />
        );

      case 'scatter':
        return (
          <Scatter
            data={safeData as unknown as Parameters<typeof Scatter>[0]['data']}
            options={{
              ...commonOptions,
              scales: {
                x: {
                  grid: { color: '#E5E7EB' },
                  ticks: { color: '#6B7280' },
                },
                y: {
                  grid: { color: '#E5E7EB' },
                  ticks: { color: '#6B7280' },
                },
              },
            }}
          />
        );

      default:
        return (
          <div className="flex items-center justify-center h-full text-gray-500">
            Unsupported chart type: {chartData.type}
          </div>
        );
    }
  }


  return (
    <div className="relative h-[22rem] pt-8 pr-6">
      {renderChart()}

      {/* Toggle Switch */}
      <div className="absolute top-1 right-5 flex items-center space-x-2 z-10">
        <span
          style={{
            fontSize: "11px",
            color: "#374151",
            fontWeight: 500,
          }}
        >
          Values
        </span>

        <label
          style={{
            position: "relative",
            display: "inline-block",
            width: "38px",
            height: "20px",
            cursor: "pointer",
          }}
        >
          <input
            type="checkbox"
            checked={showLabels}
            onChange={() => setShowLabels((prev) => !prev)}
            style={{
              opacity: 0,
              width: 0,
              height: 0,
            }}
          />
          <span
            style={{
              position: "absolute",
              top: 0,
              left: 0,
              right: 0,
              bottom: 0,
              background: showLabels
                ? "linear-gradient(135deg, #3b82f6, #2563eb)"
                : "#d1d5db",
              borderRadius: "9999px",
              transition: "background 0.3s ease",
            }}
          ></span>
          <span
            style={{
              position: "absolute",
              top: "2px",
              left: showLabels ? "20px" : "2px",
              width: "16px",
              height: "16px",
              backgroundColor: "white",
              borderRadius: "50%",
              transition: "left 0.25s ease",
              boxShadow: "0 1px 3px rgba(0, 0, 0, 0.2)",
            }}
          ></span>
        </label>
      </div>
    </div>
  );

};


export default ChartRenderer;