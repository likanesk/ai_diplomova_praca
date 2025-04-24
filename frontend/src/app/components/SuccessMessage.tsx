"use client";

import { IoCheckmarkOutline, IoClose } from "react-icons/io5";
import Button from "./Button";
import IconButton from "./IconButton";

interface SuccessMessageProps {
  onClose: () => void;
  message: string;
}

export default function SuccessMessage({
  onClose,
  message,
}: SuccessMessageProps) {
  return (
    <div
      className="fixed inset-0 z-50 flex justify-center items-center bg-black bg-opacity-50"
      onClick={onClose}
    >
      <div
        className="relative p-4 w-full max-w-md bg-white rounded-lg shadow dark:bg-gray-800"
        onClick={(e) => e.stopPropagation()}
      >
        <IconButton
          icon={<IoClose className="w-4 h-4" />}
          onClick={onClose}
          position="absolute"
          top="top-2"
          right="right-2"
        />

        <div className="p-4 text-center">
          <div className="w-12 h-12 rounded-full bg-green-100 dark:bg-green-900 p-2 flex items-center justify-center mx-auto mb-3.5">
            <IoCheckmarkOutline className="w-8 h-8 text-green-500 dark:text-green-400" />
            <span className="sr-only">Success</span>
          </div>
          <p className="mb-4 text-lg font-semibold text-gray-900 dark:text-white">
            {message}
          </p>
          <Button type="button" onClick={onClose}>
            Continue
          </Button>
        </div>
      </div>
    </div>
  );
}
