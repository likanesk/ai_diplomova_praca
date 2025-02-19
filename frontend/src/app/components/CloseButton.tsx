import { IoClose } from "react-icons/io5";

interface CloseButtonProps {
  onClose: () => void;
  className?: string;
}

export default function CloseButton({ onClose, className }: CloseButtonProps) {
  return (
    <button
      onClick={onClose}
      className={`absolute top-3 right-3 text-gray-400 hover:bg-gray-200 hover:text-gray-900 rounded-lg text-sm w-8 h-8 flex items-center justify-center dark:hover:bg-gray-600 dark:hover:text-white ${
        className || ""
      }`}
    >
      <IoClose className="w-4 h-4" />
    </button>
  );
}
