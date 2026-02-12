import { cn } from "@/lib/utils";

const activities = [
  {
    id: 1,
    type: "quality",
    message: "Data quality score improved to 98.5%",
    time: "2 minutes ago",
    status: "success",
  },
  {
    id: 2,
    type: "security",
    message: "New security policy applied to 3 tables",
    time: "15 minutes ago",
    status: "info",
  },
  {
    id: 3,
    type: "access",
    message: "Access request approved for marketing team",
    time: "1 hour ago",
    status: "success",
  },
  {
    id: 4,
    type: "observability",
    message: "Pipeline latency alert triggered",
    time: "2 hours ago",
    status: "warning",
  },
  {
    id: 5,
    type: "insights",
    message: "New dashboard created: Revenue Analysis",
    time: "3 hours ago",
    status: "info",
  },
];

export function ActivityFeed() {
  return (
    <div className="glass-card rounded-xl p-6">
      <h3 className="text-lg font-semibold text-foreground mb-4">Recent Activity</h3>
      
      <div className="space-y-4">
        {activities.map((activity) => (
          <div key={activity.id} className="flex items-start gap-3">
            <div className={cn(
              "w-2 h-2 rounded-full mt-2 flex-shrink-0",
              activity.status === "success" && "bg-success",
              activity.status === "warning" && "bg-warning",
              activity.status === "error" && "bg-destructive",
              activity.status === "info" && "bg-primary"
            )} />
            <div className="flex-1 min-w-0">
              <p className="text-sm text-foreground">{activity.message}</p>
              <p className="text-xs text-muted-foreground mt-0.5">{activity.time}</p>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
