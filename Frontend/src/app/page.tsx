"use client";
import { useAuth } from "@/contexts/AuthContext";
import {
  BarChart3,
  Shield,
  ShieldCheck,
  Key,
  Sparkles,
  Database,
  Users,
  Activity,
  AlertTriangle,
  Layers,
} from "lucide-react";
import AppLayout from "@/components/layout/AppLayout";
import { ProductCard } from "@/components/dashboard/ProductCard";
import { StatCard } from "@/components/dashboard/StatCard";
import { ActivityFeed } from "@/components/dashboard/ActivityFeed";

const products = [
  {
    name: "Data Insights",
    description: "Analyze and visualize your data with powerful dashboards and reports.",
    icon: BarChart3,
    path: "/insights",
    color: "bg-primary/20 text-primary",
    stats: { label: "Active Dashboards", value: "24", status: "success" as const },
  },
  {
    name: "Data Quality",
    description: "Monitor and improve data quality with automated validation rules.",
    icon: Sparkles,
    path: "/quality",
    color: "bg-emerald-500/20 text-emerald-400",
    stats: { label: "Quality Score", value: "98.5%", status: "success" as const },
  },
  {
    name: "Data Security",
    description: "Protect sensitive data with encryption and access controls.",
    icon: Shield,
    path: "/security",
    color: "bg-amber-500/20 text-amber-400",
    stats: { label: "Security Policies", value: "12 Active" },
  },
  {
    name: "Data Reliability",
    description: "Monitor data pipelines and get alerts on anomalies.",
    icon: ShieldCheck,
    path: "/reliability",
    color: "bg-violet-500/20 text-violet-400",
    stats: { label: "Pipeline Health", value: "2 Issues", status: "warning" as const },
  },
  {
    name: "Asset Catalog",
    description: "Manage your data assets, pipelines, and cloud resources.",
    icon: Layers,
    path: "/catalog",
    color: "bg-cyan-500/20 text-cyan-400",
    stats: { label: "Total Assets", value: "89" },
  },
  {
    name: "Access Management",
    description: "Control who can access your data with fine-grained permissions.",
    icon: Key,
    path: "/access",
    color: "bg-rose-500/20 text-rose-400",
    stats: { label: "Active Users", value: "156" },
  },
];

export default function Index() {
  const { hasAccess } = useAuth();

  return (
    <AppLayout title="Dashboard" subtitle="Welcome back, John">
      {/* Stats Overview */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatCard
          label="Total Data Assets"
          value="N/A"
          change={{ value: "+N/A", trend: "up" }}
          icon={Database}
        />
        <StatCard
          label="Active Users"
          value="N/A"
          change={{ value: "+N/A", trend: "up" }}
          icon={Users}
        />
        <StatCard
          label="Pipeline Runs Today"
          value="N/A"
          icon={Activity}
        />
        <StatCard
          label="Open Alerts"
          value="N/A"
          change={{ value: "-N/A", trend: "down" }}
          icon={AlertTriangle}
        />
      </div>

      {/* Main Content */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Products Grid */}
        <div className="lg:col-span-2">
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {products.map((product) => (
              <ProductCard 
                key={product.name} 
                {...product} 
                locked={!hasAccess(product.path)}
              />
            ))}
          </div>
        </div>

        {/* Activity Feed */}
        <div className="lg:col-span-1">
          <ActivityFeed />
        </div>
      </div>
    </AppLayout>
  );
}
