import { useState, useEffect } from "react";
import {
  BarChart3,
  Shield,
  Eye,
  Key,
  Sparkles,
  Network,
  Settings,
  Activity,
  ChevronLeft,
  ChevronRight,
  Layers,
  Home,
  DollarSign,
  Lock,
  
  PieChart,
  GitBranch,
  ChevronRightIcon,
  Users,
  
  Bot,
  Scale,
  Wrench,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { useAuth } from "@/contexts/AuthContext";
import { useProductSettings } from "@/contexts/ProductSettingsContext";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { DiceLogo } from "@/components/icons/DiceLogo";
import { SidebarSubmenu } from "./SidebarSubmenu";
import Link from "next/link";
import { usePathname } from "next/navigation";

const productSubmenus: Record<string, { name: string; path: string; hasSubmenu?: boolean }[]> = {
  "/catalog": [
    { name: "Data Foundations", path: "/catalog/data-foundations" },
    { name: "Cloud Infrastructure", path: "/catalog/cloud" },
    { name: "Data catalog tools", path: "/catalog/catalog-tools" },
    { name: "Streaming Infrastructure", path: "/catalog/streaming" },
    { name: "Compute Services", path: "/catalog/compute" },
    { name: "Orchestration Services", path: "/catalog/orchestration" },
    { name: "Code Repos", path: "/catalog/code-repos" },
    { name: "CI/CD", path: "/catalog/cicd" },
    { name: "Tech Radar", path: "/catalog/tech-radar" },
  ],
  "/cost-insights": [
    { name: "Overview", path: "/cost-insights" },
    { name: "Budgeting", path: "/budgeting" },
    { name: "Cost Insights", path: "/cost-insights?view=breakdown" },
    { name: "Optimization", path: "/cost-optimization" },
  ],
  "/security": [
    { name: "Policies", path: "/security?tab=policies" },
    { name: "Infrastructure Security", path: "/security/threat-intel" },
    { name: "Backup & Recovery", path: "/security/backup" },
    { name: "Secrets Management", path: "/security/secrets" },
    { name: "Audit Log", path: "/security?tab=audit" },
  ],
  "/governance": [
    { name: "Governance Policies", path: "/governance/policies" },
    { name: "Data Stewardship", path: "/governance/stewardship" },
    { name: "Business Glossary", path: "/governance/glossary" },
    { name: "Data Catalog", path: "/governance/data-catalog" },
    { name: "Metadata Catalog", path: "/governance/metadata-catalog" },
    { name: "Data Classification & Discovery", path: "/governance/classification" },
    { name: "Classification Rules", path: "/governance/classification/rules" },
  ],
  "/access": [
    { name: "Overview", path: "/access" },
    { name: "Request Access", path: "/access/request" },
    { name: "Agentic Access", path: "/access/agentic" },
    { name: "User Access Reviews", path: "/access/uar" },
  ],
  "/quality": [
    { name: "Overview", path: "/quality" },
    { name: "Quality Rules", path: "/quality/rules" },
  ],
  "/observability": [
    { name: "Overview", path: "/observability" },
    { name: "RCA Collection", path: "/observability/rca" },
  ],
  "/misc-tools": [
    { name: "Data Profiler", path: "/misc-tools/data-profiler" },
    // { name: "Data Synthesizer", path: "/misc-tools/data-synthesizer" },
  ],
};

const products = [
  { name: "Dashboard", icon: Home, path: "/", color: "text-foreground", hasSubmenu: false },
  { name: "DICE Intelligence", icon: Bot, path: "/ai-agent", color: "text-indigo-400", hasSubmenu: false },
  { name: "Data Organization", icon: Network, path: "/users", color: "text-purple-400", hasSubmenu: false },
  { name: "Asset Catalog", icon: Layers, path: "/catalog", color: "text-sky-400", hasSubmenu: true },
  { name: "Data Lineage", icon: GitBranch, path: "/lineage", color: "text-cyan-400", hasSubmenu: false },
  { name: "Consumption Analytics", icon: PieChart, path: "/consumption", color: "text-orange-400", hasSubmenu: false },
  { name: "Data Insights", icon: BarChart3, path: "/insights", color: "text-primary", hasSubmenu: false },
  { name: "Cost Governance", icon: DollarSign, path: "/cost-insights", color: "text-teal-400", hasSubmenu: true },
  { name: "Data Quality", icon: Sparkles, path: "/quality", color: "text-emerald-400", hasSubmenu: true },
  { name: "Data Governance", icon: Scale, path: "/governance", color: "text-lime-400", hasSubmenu: true },
  { name: "Data Security", icon: Shield, path: "/security", color: "text-amber-400", hasSubmenu: true },
  { name: "Platform Reliability", icon: Eye, path: "/observability", color: "text-violet-400", hasSubmenu: true },
  { name: "Access Management", icon: Key, path: "/access", color: "text-rose-400", hasSubmenu: true },
  { name: "Productivity Tools", icon: Wrench, path: "/misc-tools", color: "text-zinc-400", hasSubmenu: true },
];

const common = [
  { name: "User Management", icon: Users, path: "/user-management", hasSubmenu: false },
  { name: "Settings", icon: Settings, path: "/settings", hasSubmenu: false },
  { name: "Usage", icon: Activity, path: "/usage", hasSubmenu: false },
];

export function AppSidebar() {
  const [collapsed, setCollapsed] = useState(false);
  const [activeSubmenu, setActiveSubmenu] = useState<string | null>(null);
  const location = usePathname();
  const { hasAccess } = useAuth();
  const { isProductVisible, isProductLocked } = useProductSettings();

  const isActive = (path: string) => location === path || location.startsWith(path + "/");

  // Determine which product submenu should be open based on current route
  const getActiveProductPath = () => {
    for (const product of products) {
      if (product.hasSubmenu && productSubmenus[product.path]) {
        const submenuItems = productSubmenus[product.path];
        for (const item of submenuItems) {
          // Check if current path matches any submenu item
          if (location === item.path || 
              location.startsWith(item.path.split('?')[0]) ||
              (item.path.includes('?') && location + location.search === item.path)) {
            return product.path;
          }
        }
        // Also check if we're on a child route of the product
        if (location.startsWith(product.path + "/")) {
          return product.path;
        }
      }
    }
    return null;
  };

  // Keep submenu open based on current route
  const effectiveActiveSubmenu = activeSubmenu !== null ? activeSubmenu : getActiveProductPath();

  // Auto-collapse when navigating to a submenu route
  useEffect(() => {
    const productPath = getActiveProductPath();
    if (productPath && !activeSubmenu) {
      setCollapsed(true);
    }
  }, [location]);

  const handleNavClick = (item: typeof products[0]) => {
    if (item.hasSubmenu && productSubmenus[item.path]) {
      if (activeSubmenu === item.path) {
        // Clicking same menu - close it
        setActiveSubmenu(null);
        setCollapsed(false);
      } else {
        setActiveSubmenu(item.path);
        setCollapsed(true);
      }
    }
  };

  const handleCloseSubmenu = () => {
    setActiveSubmenu(null);
    setCollapsed(false);
  };

  const renderNavItem = (item: typeof products[0]) => {
    const canAccess = hasAccess(item.path);
    const hasSubmenu = item.hasSubmenu && productSubmenus[item.path];
    
    if (!canAccess) {
      return (
        <Tooltip key={item.name}>
          <TooltipTrigger asChild>
            <div
              className={cn(
                "sidebar-item opacity-50 cursor-not-allowed"
              )}
            >
              <item.icon className="w-5 h-5 flex-shrink-0 text-muted-foreground" />
              {!collapsed && (
                <>
                  <span className="flex-1 text-muted-foreground">{item.name}</span>
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

    const isLocked = isProductLocked(item.path);

    // Locked items - show as non-interactive with lock icon
    if (isLocked) {
      return (
        <Tooltip key={item.name}>
          <TooltipTrigger asChild>
            <div
              className={cn(
                "sidebar-item opacity-60 cursor-not-allowed"
              )}
            >
              <item.icon className="w-5 h-5 flex-shrink-0 text-muted-foreground" />
              {!collapsed && (
                <>
                  <span className="flex-1 text-muted-foreground">{item.name}</span>
                  <Lock className="w-4 h-4 text-muted-foreground" />
                </>
              )}
            </div>
          </TooltipTrigger>
          <TooltipContent side="right">
            <p>Locked - unlock in Settings</p>
          </TooltipContent>
        </Tooltip>
      );
    }

    if (hasSubmenu) {
      const active = isActive(item.path);
      const button = (
        <button
          key={item.name}
          onClick={() => handleNavClick(item)}
          className={cn(
            "sidebar-item w-full",
            active && "sidebar-item-active"
          )}
        >
          <item.icon className="w-5 h-5 flex-shrink-0" />
          {!collapsed && (
            <>
              <span className="flex-1 text-left">{item.name}</span>
              <ChevronRightIcon className="w-4 h-4 opacity-60" />
            </>
          )}
        </button>
      );

      if (collapsed) {
        return (
          <Tooltip key={item.name}>
            <TooltipTrigger asChild>
              {button}
            </TooltipTrigger>
            <TooltipContent side="right">
              <p>{item.name}</p>
            </TooltipContent>
          </Tooltip>
        );
      }
      return button;
    }

    const active = isActive(item.path);
    const navLink = (
      <Link
        key={item.name}
        href={item.path}
        className={cn(
          "sidebar-item",
          active && "sidebar-item-active"
        )}
      >
        <item.icon className="w-5 h-5 flex-shrink-0" />
        {!collapsed && <span className="flex-1">{item.name}</span>}
      </Link>
    );

    if (collapsed) {
      return (
        <Tooltip key={item.name}>
          <TooltipTrigger asChild>
            {navLink}
          </TooltipTrigger>
          <TooltipContent side="right">
            <p>{item.name}</p>
          </TooltipContent>
        </Tooltip>
      );
    }
    return navLink;
  };

  const activeProduct = products.find(p => p.path === effectiveActiveSubmenu);

  return (
    <div className="flex h-screen">
      <aside
        className={cn(
          "flex flex-col h-screen bg-sidebar border-r border-sidebar-border transition-all duration-300",
          collapsed ? "w-16" : "w-64"
        )}
      >
      {/* Logo */}
      <div className="flex items-center gap-3 px-4 py-5 border-b border-sidebar-border">
        <div className="w-8 h-8 rounded-lg bg-primary flex items-center justify-center">
          <DiceLogo size={20} className="text-primary-foreground" />
        </div>
        {!collapsed && (
          <div className="flex flex-col">
            <span className="font-semibold text-foreground text-lg">DICE</span>
            <span className="text-[10px] text-muted-foreground leading-tight">Data, Intelligence & Cognitive Engine</span>
          </div>
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
            {products
              .filter((item) => item.path === "/" || isProductVisible(item.path))
              .map((item) => renderNavItem(item))}
          </nav>
        </div>

        <div className="border-t border-sidebar-border pt-4">
          {!collapsed && (
            <span className="text-xs font-medium text-muted-foreground uppercase tracking-wider px-3 mb-2 block">
              Platform
            </span>
          )}
          <nav className="space-y-1 mt-2">
            {common.map((item) => renderNavItem({ ...item, color: "" }))}
          </nav>
        </div>
      </div>

        {/* Collapse/Expand Toggle */}
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

      {/* Second Level Submenu Panel */}
      {effectiveActiveSubmenu && activeProduct && productSubmenus[effectiveActiveSubmenu] && (
        <SidebarSubmenu
          title={activeProduct.name}
          items={productSubmenus[effectiveActiveSubmenu]}
          nestedSubmenus={productSubmenus}
          onClose={handleCloseSubmenu}
        />
      )}
    </div>
  );
}
