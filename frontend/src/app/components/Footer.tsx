import Image from "next/image";
import Link from "next/link";

export default function Footer() {
  return (
    <footer className="bg-white rounded-lg shadow-sm dark:bg-gray-900 m-4">
      <div className="w-full max-w-screen-xl mx-auto p-4 md:py-8">
        <div className="sm:flex sm:items-center sm:justify-between">
          <div className="flex items-center mb-4 sm:mb-0 space-x-3 rtl:space-x-reverse">
            <a href="https://fastapi.tiangolo.com/">
              <Image
                src="/fastapi-icon.png"
                width={25}
                height={25}
                alt="FastAPI Logo"
              />
            </a>
            <span className="self-center text-2xl font-semibold whitespace-nowrap dark:text-white">
              API for medical data
            </span>
          </div>
          <ul className="flex flex-wrap items-center mb-6 text-sm font-medium text-gray-500 sm:mb-0 dark:text-gray-400">
            <li>
              <Link href="/pages/home" className="hover:underline me-4 md:me-6">
                Home
              </Link>
            </li>
            <li>
              <a
                href="https://mit-license.org/"
                className="hover:underline me-4 md:me-6"
              >
                Licensing
              </a>
            </li>
            <li>
              <a
                href="https://github.com/likanesk/ai_diplomova_praca"
                className="hover:underline"
              >
                GitHub
              </a>
            </li>
          </ul>
        </div>
        <hr className="my-6 border-gray-200 sm:mx-auto dark:border-gray-700 lg:my-8" />
        <span className="block text-sm text-gray-500 sm:text-center dark:text-gray-400">
          © 2025 API for medical data
        </span>
      </div>
    </footer>
  );
}
