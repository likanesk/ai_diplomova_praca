import "./globals.css";
import Footer from "./components/Footer";
import Navbar from "./components/Navbar";
import { AuthProvider } from "./context/AuthContext";
import { Suspense } from "react";

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="h-full">
      <body className="h-full flex flex-col">
        <AuthProvider>
          <Suspense>
            <Navbar />
            <main className="flex-grow">{children}</main>
            <Footer />
          </Suspense>
        </AuthProvider>
      </body>
    </html>
  );
}
