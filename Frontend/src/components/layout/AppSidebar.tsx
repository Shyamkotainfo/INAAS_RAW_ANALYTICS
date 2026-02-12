import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import {
  BarChart3,
  Shield,
  Eye,
  Key,
  Sparkles,
  Users,
  Settings,
  Activity,
  ChevronLeft,
  ChevronRight,
  Database,
  Layers,
  Home,
  DollarSign,
  Lock,
  PieChart,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useAuth } from "@/contexts/AuthContext";
import { Tooltip, TooltipContent, TooltipTrigger } from "../ui/tooltip";
// import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";

const products = [
  { name: "Dashboard", icon: Home, path: "/", color: "text-foreground" },
  {
    name: "Asset Catalog",
    icon: Layers,
    path: "/catalog",
    color: "text-sky-400",
  },
  {
    name: "Data Insights",
    icon: BarChart3,
    path: "/insights",
    color: "text-primary",
  },
  {
    name: "Cost Insights",
    icon: DollarSign,
    path: "/cost-insights",
    color: "text-teal-400",
  },
  {
    name: "Consumption Analytics",
    icon: PieChart,
    path: "/consumption",
    color: "text-orange-400",
  },
  {
    name: "Data Quality",
    icon: Sparkles,
    path: "/quality",
    color: "text-emerald-400",
  },
  {
    name: "Data Security",
    icon: Shield,
    path: "/security",
    color: "text-amber-400",
  },
  {
    name: "Data Reliability",
    icon: Eye,
    path: "/observability",
    color: "text-violet-400",
  },
  {
    name: "Access Management",
    icon: Key,
    path: "/access",
    color: "text-rose-400",
  },
];

const common = [
  { name: "User Management", icon: Users, path: "/users" },
  { name: "Settings", icon: Settings, path: "/settings" },
  { name: "Usage", icon: Activity, path: "/usage" },
];

export function AppSidebar() {
  const [collapsed, setCollapsed] = useState(false);
  const location = usePathname();
  const { hasAccess } = useAuth();

  const isActive = (path: string) => location === path;

  const renderNavItem = (item: (typeof products)[0], showColor = true) => {
    const canAccess = hasAccess(item.path);
    if (!canAccess) {
      return (
        <Tooltip key={item.name}>
          <TooltipTrigger asChild>
            <div className={cn("sidebar-item opacity-50 cursor-not-allowed")}>
              <item.icon className="w-5 h-5 flex-shrink-0 text-muted-foreground" />
              {!collapsed && (
                <>
                  <span className="flex-1 text-muted-foreground">
                    {item.name}
                  </span>
                  <Lock className="w-4 h-4 text-muted-foreground" />
                </>
              )}
            </div>
          </TooltipTrigger>
          <TooltipContent side="right">
            <p>Access restricted</p>
          </TooltipContent>
        </Tooltip>
      );
    }

    return (
      <Link
        key={item.name}
        href={item.path}
        className={cn(
          "sidebar-item",
          isActive(item.path) && "sidebar-item-active"
        )}
      >
        <item.icon
          className={cn(
            "w-5 h-5 flex-shrink-0",
            showColor && isActive(item.path) ? item.color : ""
          )}
        />
        {!collapsed && <span>{item.name}</span>}
      </Link>
    );
  };

  return (
    <aside
      className={cn(
        "flex flex-col h-screen bg-sidebar border-r border-sidebar-border transition-all duration-300",
        collapsed ? "w-16" : "w-64"
      )}
    >
      {/* Logo */}
      <div className="flex items-center gap-3 px-4 py-5 border-b border-sidebar-border">
        <div className="w-8 h-8 rounded-lg bg-primary flex items-center justify-center">
          <Database className="w-5 h-5 text-primary-foreground" />
        </div>
        {!collapsed && (
          <span className="font-semibold text-foreground text-lg">DataHub</span>
        )}
      </div>

      {/* Products */}
      <div className="flex-1 overflow-y-auto py-4 px-3">
        <div className="mb-6">
          {!collapsed && (
            <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider px-3 mb-2 block">
              Products
            </span>
          )}
          <nav className="space-y-1 mt-2">
            {products.map((item) => renderNavItem(item))}
          </nav>
        </div>

        <div className="border-t border-sidebar-border pt-4">
          {!collapsed && (
            <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider px-3 mb-2 block">
              Platform
            </span>
          )}
          <nav className="space-y-1 mt-2">
            {common.map((item) => renderNavItem({ ...item, color: "" }, false))}
          </nav>
        </div>
      </div>

      {/* Collapse Toggle */}
      <div className="p-3 border-t border-sidebar-border">
        <button
          onClick={() => setCollapsed(!collapsed)}
          className="sidebar-item w-full justify-center"
        >
          {collapsed ? (
            <ChevronRight className="w-5 h-5" />
          ) : (
            <>
              <ChevronLeft className="w-5 h-5" />
              <span>Collapse</span>
            </>
          )}
        </button>
      </div>
    </aside>
  );
}
