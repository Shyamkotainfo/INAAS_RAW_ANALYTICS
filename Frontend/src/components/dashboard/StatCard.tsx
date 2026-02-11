import { LucideIcon, TrendingUp, TrendingDown } from "lucide-react";
import { cn } from "@/lib/utils";

interface StatCardProps {
  label: string;
  value: string;
  change?: {
    value: string;
    trend: "up" | "down";
  };
  icon: LucideIcon;
}

export function StatCard({ label, value, change, icon: Icon }: StatCardProps) {
  return (
    <div className="stat-card">
      <div className="flex items-center justify-between mb-3">
        <span className="text-sm text-muted-foreground">{label}</span>
        <Icon className="w-4 h-4 text-muted-foreground" />
      </div>
      
      <div className="flex items-end justify-between">
        <span className="text-2xl font-bold text-foreground">{value}</span>
        {change && (
          <div className={cn(
            "flex items-center gap-1 text-sm",
            change.trend === "up" ? "text-success" : "text-destructive"
          )}>
            {change.trend === "up" ? (
              <TrendingUp className="w-4 h-4" />
            ) : (
              <TrendingDown className="w-4 h-4" />
            )}
            <span>{change.value}</span>
          </div>
        )}
      </div>
    </div>
  );
}
