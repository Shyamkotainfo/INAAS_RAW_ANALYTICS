"use client";
import AppLayout from "@/components/layout/AppLayout";
import { StatCard } from "@/components/dashboard/StatCard";
import { Button } from "@/components/ui/button";
import { BarChart3, TrendingUp, Eye, FileText, Plus, Lock } from "lucide-react";
import { useRouter } from "next/navigation";

const myDashboards = [
  { name: "Revenue Analysis", slug: "revenue-analysis" },
  { name: "Customer Metrics", slug: "customer-metrics" },
  { name: "Product Performance", slug: "product-performance" },
  { name: "Sales Pipeline", slug: "sales-pipeline" },
];

const sharedDashboards = [
  { name: "Marketing Overview", slug: "marketing-overview", owner: "Sarah M." },
  {
    name: "Q4 Financial Summary",
    slug: "q4-financial-summary",
    owner: "John D.",
  },
  {
    name: "User Engagement Report",
    slug: "user-engagement-report",
    owner: "Alex K.",
  },
];

export default function DataInsights() {
  const router = useRouter();

  return (
    <AppLayout title="Data Insights" subtitle="Analyze and visualize your data">
      <div className="flex justify-end mb-6">
        <Button onClick={() => router.push("/insights/new")} className="gap-2">
          <Plus className="w-4 h-4" />
          New Insight
        </Button>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4 mb-8">
        <StatCard
          label="Saved Dashboards"
          value="N/A"
          icon={BarChart3}
          change={{ value: "+N/A", trend: "up" }}
        />
        <StatCard label="Schedules" value="N/A" icon={FileText} />
        <StatCard
          label="Views Today"
          value="N/A"
          icon={Eye}
          change={{ value: "N/A", trend: "up" }}
        />
        <StatCard label="Insights Generated" value="N/A" icon={TrendingUp} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <div className="glass-card rounded-xl p-6">
          <h2 className="text-lg font-semibold text-foreground mb-4">
            My dashboards
          </h2>
          <div className="space-y-4">
            {myDashboards.map((dashboard, i) => (
              <div
                key={dashboard.slug}
                // onClick={() => router.push(`/insights/dashboard/${dashboard.slug}`)}
                className="flex items-center justify-between p-4 rounded-lg bg-secondary/50 hover:bg-secondary transition-colors opacity-60 cursor-not-allowed"
              >
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-lg bg-primary/20 flex items-center justify-center">
                    <BarChart3 className="w-5 h-5 text-primary" />
                  </div>
                  <div>
                    <p className="font-medium text-foreground">
                      {dashboard.name}
                    </p>
                    <p className="text-sm text-muted-foreground">
                      Updated {i + 1} hour{i > 0 ? "s" : ""} ago
                    </p>
                  </div>
                </div>
                <span className="flex text-sm text-muted-foreground">
                  {Math.floor(Math.random() * 50 + 10)} views{" "}
                  <Lock className="w-5 ms-2 h-5 text-muted-foreground" />
                </span>
              </div>
            ))}
          </div>
        </div>

        <div className="glass-card rounded-xl p-6">
          <h2 className="text-lg font-semibold text-foreground mb-4">
            Dashboards shared with me
          </h2>
          <div className="space-y-4">
            {sharedDashboards.map((item, i) => (
              <div
                key={item.slug}
                // onClick={() => router.push(`/insights/dashboard/${item.slug}`)}
                className="flex items-center justify-between p-4 rounded-lg bg-secondary/50 hover:bg-secondary transition-colors opacity-60 cursor-not-allowed"
              >
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-lg bg-accent/20 flex items-center justify-center">
                    <BarChart3 className="w-5 h-5 text-accent-foreground" />
                  </div>
                  <div>
                    <p className="font-medium text-foreground">{item.name}</p>
                    <p className="text-sm text-muted-foreground">
                      Shared by {item.owner}
                    </p>
                  </div>
                </div>
                <span className="flex text-sm text-muted-foreground">
                  {i + 2} days ago{" "}
                  
                </span>
                 
              </div>
            ))}
          </div>
        </div>
      </div>
    </AppLayout>
  );
}
