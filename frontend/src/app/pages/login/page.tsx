"use client";

import { useState, FormEvent } from "react";
import { useRouter } from "next/navigation";
import Button from "@/app/components/Button";
import { callLogin } from "@/app/services/auth/authService";
import InputField from "@/app/components/InputField";
import ErrorMessage from "@/app/components/ErrorMessage";
import { useAuth } from "@/app/context/AuthContext";

export default function LoginPage() {
  const router = useRouter();
  const { login } = useAuth();
  const [username, setUsername] = useState<string>("");
  const [password, setPassword] = useState<string>("");
  const [error, setError] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(false);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError("");

    try {
      const { access_token } = await callLogin(username, password);
      login(access_token);
      router.push("/pages/home");
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "An unknown error occurred"
      );
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="p-5 text-lg font-semibold text-left rtl:text-right text-gray-900 bg-white dark:text-white dark:bg-gray-800">
      <div className="max-w-sm mx-auto my-4">
        <h1 className="text-2xl pb-5 text-center">Login page</h1>
      </div>

      <section>
        <div className="flex flex-col items-center mx-auto">
          <a
            href="#"
            className="flex items-center mb-6 text-2xl font-semibold text-gray-900 dark:text-white"
          ></a>
          <div className="w-full bg-white rounded-lg shadow dark:border md:mt-0 sm:max-w-md xl:p-0 dark:bg-gray-800 dark:border-gray-700">
            <div className="p-6 space-y-4 md:space-y-6 sm:p-8">
              <h1 className="leading-tight tracking-tight text-gray-900 md:text-2dl dark:text-white">
                Sign in to your account
              </h1>
              <form className="space-y-4 md:space-y-6" onSubmit={handleSubmit}>
                <InputField
                  label="Your username"
                  type="text"
                  id="username"
                  placeholder="username"
                  required
                  value={username}
                  onChange={(value) => setUsername(value)}
                />
                <InputField
                  label="Password"
                  type="password"
                  id="password"
                  placeholder="••••••••"
                  required
                  value={password}
                  onChange={(value) => setPassword(value)}
                />
                {error && (
                  <ErrorMessage message={error} onClose={() => setError("")} />
                )}
                <Button type="submit" disabled={loading} className="w-full">
                  {loading ? "Loading..." : "Sign in"}
                </Button>
              </form>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
