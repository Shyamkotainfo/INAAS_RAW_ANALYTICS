// "use client";
// import { useEffect, useState } from "react";
// import { useRouter } from "next/navigation";

// export default function AuthGuard({ children }: { children: React.ReactNode }) {
//   const router = useRouter();
//   const [isAuthChecked, setIsAuthChecked] = useState(false);

//   useEffect(() => {
//     const auth = localStorage.getItem("isAuthenticated");
//     if (!auth) {
//       router.replace("/");
//     } else {
//       setIsAuthChecked(true);
//     }
//   }, [router]);

//   // wait until check is done before rendering children
//   if (!isAuthChecked) return null;

//   return <>{children}</>;
// }


