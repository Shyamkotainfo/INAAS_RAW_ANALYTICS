
// "use client";
// import { useRouter } from "next/navigation";
// import { useState } from "react";
// import Image from "next/image";
// import logo from "../asserts/info-sevices-logo.png"; // ✅ ensure the path is correct

// export default function LoginPage() {
//   const router = useRouter();
//   const [username, setUsername] = useState("");
//   const [password, setPassword] = useState("");

//   const handleLogin = (e: React.FormEvent) => {
//     e.preventDefault();

//     if (username === "admin" && password === "1234") {
//       localStorage.setItem("isAuthenticated", "true");
//       router.push("/chat");
//     } else {
//       alert("Invalid credentials");
//     }
//   };

//   return (
//     <div className="min-h-screen flex flex-col bg-white relative overflow-hidden">
//       {/* Header */}
//       <header
//         className="bg-white shadow-sm sticky top-0 z-30"
//         style={{ borderBottom: "2px solid #e0e0e0" }}
//       >
//         <div style={{ padding: "10px" }}>
//           <div className="flex items-center space-x-0">
//             <Image
//               src={logo}
//               alt="IAAS BI BOT Logo"
//               height={35}
//               className="object-contain"
//               priority
//             />
//           </div>
//         </div>
//       </header>

//       {/* Centered Login Card */}
//       <div className="flex flex-1 items-center justify-center relative z-10">
//         <div className="bg-white shadow-md rounded-2xl p-8 w-full max-w-md border border-gray-200">
//           <h2 className="text-3xl font-semibold text-center text-gray-800 mb-2 tracking-tight">
//             Login
//           </h2>
//           <p className="text-center text-gray-500 mb-8 text-sm">
//             Please sign in to continue
//           </p>

//           <form onSubmit={handleLogin} className="flex flex-col gap-6">
//             <div>
//               <label className="block text-sm font-medium text-gray-700 mb-1">
//                 Username
//               </label>
//               <input
//                 type="text"
//                 placeholder="Enter your username"
//                 value={username}
//                 onChange={(e) => setUsername(e.target.value)}
//                 className="w-full border border-gray-300 px-4 py-2 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition"
//               />
//             </div>

//             <div>
//               <label className="block text-sm font-medium text-gray-700 mb-1">
//                 Password
//               </label>
//               <input
//                 type="password"
//                 placeholder="Enter your password"
//                 value={password}
//                 onChange={(e) => setPassword(e.target.value)}
//                 className="w-full border border-gray-300 px-4 py-2 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition"
//               />
//             </div>

//             <button
//               type="submit"
//               className="bg-blue-600 text-white py-2.5 rounded-md font-semibold hover:bg-blue-700 hover:shadow-lg transition-all duration-200"
//             >
//               Login
//             </button>
//             <div>userName:admin,password:1234</div>
//           </form>
//         </div>
//       </div>
//     </div>
//   );
// }
