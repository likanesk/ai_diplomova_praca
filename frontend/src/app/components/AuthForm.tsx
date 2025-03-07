"use client";

import { useState, FormEvent } from "react";
import { useRouter } from "next/navigation";
import { callLogin, callRegister } from "../services/auth/authService";
import Button from "./Button";
import InputField from "./InputField";
import CustomLink from "./CustomLink";
import ErrorMessage from "./ErrorMessage";

interface AuthFormProps {
  isLogin: boolean;
}

export default function AuthForm({ isLogin }: AuthFormProps) {
  const router = useRouter();
  const [username, setUsername] = useState<string>("");
  const [password, setPassword] = useState<string>("");
  const [confirmPassword, setConfirmPassword] = useState<string>("");
  const [error, setError] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(false);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError("");

    try {
      if (!isLogin && password !== confirmPassword) {
        throw new Error("Passwords do not match");
      }

      if (isLogin) {
        const { access_token } = await callLogin(username, password);
        localStorage.setItem("access_token", access_token);
        router.push("/pages/home");
      } else {
        await callRegister(username, password);
        router.push("/pages/login");
      }
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "An unknown error occurred"
      );
    } finally {
      setLoading(false);
    }
  };

  return (
    <section className="bg-gray-50 dark:bg-gray-900">
      <div className="flex flex-col items-center justify-center px-6 py-8 mx-auto md:h-screen lg:py-0">
        <a
          href="#"
          className="flex items-center mb-6 text-2xl font-semibold text-gray-900 dark:text-white"
        ></a>
        <div className="w-full bg-white rounded-lg shadow dark:border md:mt-0 sm:max-w-md xl:p-0 dark:bg-gray-800 dark:border-gray-700">
          <div className="p-6 space-y-4 md:space-y-6 sm:p-8">
            <h1 className="text-xl font-bold leading-tight tracking-tight text-gray-900 md:text-2dl dark:text-white">
              {isLogin ? "Sign in to your account" : "Create a new account"}
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
              {!isLogin && (
                <InputField
                  label="Confirm Password"
                  type="password"
                  id="confirmPassword"
                  placeholder="••••••••"
                  required
                  value={confirmPassword}
                  onChange={(value) => setConfirmPassword(value)}
                />
              )}
              {error && (
                <ErrorMessage message={error} onClose={() => setError("")} />
              )}
              <Button type="submit" disabled={loading} className="w-full">
                {loading ? "Loading..." : isLogin ? "Sign in" : "Sign up"}
              </Button>
              <p className="text-sm font-light text-gray-500 dark:text-gray-400">
                {isLogin ? (
                  <>
                    Don’t have an account yet?{" "}
                    <CustomLink
                      href="/pages/register"
                      className="text-primary-600 hover:underline dark:text-primary-500"
                    >
                      Sign up
                    </CustomLink>
                  </>
                ) : (
                  <>
                    Already have an account?{" "}
                    <CustomLink
                      href="/pages/login"
                      className="text-primary-600 hover:underline dark:text-primary-500"
                    >
                      Sign in
                    </CustomLink>
                  </>
                )}
              </p>
            </form>
          </div>
        </div>
      </div>
    </section>
  );
}
