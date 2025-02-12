"use client";

import { IoAlertCircleOutline, IoClose } from "react-icons/io5";

interface RemoveDialogProps {
  isOpen: boolean;
  onClose: () => void;
  onConfirm: () => void;
  name: string;
}

export default function RemoveDialog({
  isOpen,
  onClose,
  onConfirm,
  name,
}: RemoveDialogProps) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center w-full h-full bg-black bg-opacity-50">
      <div className="relative p-4 w-full max-w-md bg-white rounded-lg shadow-lg dark:bg-gray-700">
        <button
          onClick={onClose}
          className="absolute top-3 right-3 text-gray-400 hover:bg-gray-200 hover:text-gray-900 rounded-lg text-sm w-8 h-8 flex items-center justify-center dark:hover:bg-gray-600 dark:hover:text-white"
        >
          <IoClose className="w-4 h-4" />
        </button>
        <div className="p-5 text-center">
          <div className="flex flex-col items-center justify-center">
            <IoAlertCircleOutline className="w-11 h-11 text-gray-500 dark:text-gray-400" />
            <h3 className="mb-5 mt-2 text-lg font-normal text-gray-500 dark:text-gray-400">
              Are you sure you want to delete &quot;{name}&quot;?
            </h3>
          </div>

          <button
            onClick={onConfirm}
            className="text-white bg-red-600 hover:bg-red-800 focus:ring-4 focus:outline-none focus:ring-red-300 dark:focus:ring-red-800 font-medium rounded-lg text-sm inline-flex items-center px-5 py-2.5 text-center"
          >
            Yes, delete
          </button>
          <button
            onClick={onClose}
            className="py-2.5 px-5 ml-3 text-sm font-medium text-gray-900 focus:outline-none bg-white rounded-lg border border-gray-200 hover:bg-gray-100 hover:text-blue-700 focus:z-10 focus:ring-4 focus:ring-gray-100 dark:focus:ring-gray-700 dark:bg-gray-800 dark:text-gray-400 dark:border-gray-600 dark:hover:text-white dark:hover:bg-gray-700"
          >
            No, cancel
          </button>
        </div>
      </div>
    </div>
  );
}
