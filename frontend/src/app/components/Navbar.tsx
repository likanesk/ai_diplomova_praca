"use client";

import Link from "next/link";
import {
  IoBasketOutline,
  IoCameraOutline,
  IoFolderOpenOutline,
  IoHomeOutline,
  IoLogInOutline,
  IoLogOutOutline,
  IoServerOutline,
} from "react-icons/io5";
import CustomLink from "./CustomLink";
import { useAuth } from "../context/AuthContext";

export default function Navbar() {
  const { isAuthenticated, logout } = useAuth();

  return (
    <>
      <nav className="bg-white border-gray-200 dark:bg-gray-900">
        <div className="flex flex-wrap justify-between items-center mx-auto max-w-screen-xl p-4">
          <Link
            href="/pages/home"
            className="flex items-center space-x-3 rtl:space-x-reverse"
          >
            <IoHomeOutline className="w-6 h-6 text-gray-800 dark:text-white mr-1" />
            <span className="self-center text-2xl font-semibold whitespace-nowrap dark:text-white">
              Home
            </span>
          </Link>
          <div className="flex items-center space-x-6 rtl:space-x-reverse">
            {isAuthenticated ? (
              <CustomLink
                href="#"
                icon={
                  <IoLogOutOutline className="w-5 h-5 text-blue-500 dark:text-white" />
                }
                iconPosition="left"
                useFlex
                onClick={logout}
              >
                Logout
              </CustomLink>
            ) : (
              <CustomLink
                href="/pages/login"
                icon={
                  <IoLogInOutline className="w-5 h-5 text-blue-500 dark:text-white" />
                }
                iconPosition="left"
                useFlex
              >
                Login
              </CustomLink>
            )}
          </div>
        </div>
      </nav>
      <nav className="bg-gray-50 dark:bg-gray-700">
        <div className="max-w-screen-xl px-4 py-3 mx-auto">
          <div className="flex items-center">
            <ul className="flex flex-row font-medium mt-0 space-x-8 rtl:space-x-reverse text-sm">
              <li>
                <Link
                  href="/pages/bucket"
                  className="text-gray-900 dark:text-white hover:underline"
                  aria-current="page"
                >
                  <div className="flex">
                    <IoBasketOutline className="w-5 h-5 text-gray-800 dark:text-white mr-1" />
                    Buckets
                  </div>
                </Link>
              </li>
              <li>
                <Link
                  href="/pages/upload"
                  className="text-gray-900 dark:text-white hover:underline"
                >
                  <div className="flex">
                    <IoServerOutline className="w-5 h-5 text-gray-800 dark:text-white mr-1" />
                    Upload
                  </div>
                </Link>
              </li>
              <li>
                <Link
                  href="/pages/dataset"
                  className="text-gray-900 dark:text-white hover:underline"
                >
                  <div className="flex">
                    <IoFolderOpenOutline className="w-5 h-5 text-gray-800 dark:text-white mr-1" />
                    Datasets
                  </div>
                </Link>
              </li>
              <li>
                <Link
                  href="/pages/sample"
                  className="text-gray-900 dark:text-white hover:underline"
                >
                  <div className="flex">
                    <IoCameraOutline className="w-5 h-5 text-gray-800 dark:text-white mr-1" />
                    Samples
                  </div>
                </Link>
              </li>
            </ul>
          </div>
        </div>
      </nav>
    </>
  );
}
