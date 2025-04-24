"use client";

import RemoveDialog from "@/app/components/RemoveDialog";
import { useEffect, useState, useCallback } from "react";
import { IoTrashOutline } from "react-icons/io5";
import {
  callGetAllBuckets,
  callRemoveBucket,
  callCreateBucket,
} from "@/app/services/bucket/bucketService";
import Table from "@/app/components/Table";
import TableRow from "@/app/components/TableRow";
import { useRouter } from "next/navigation";
import InputField from "@/app/components/InputField";
import SuccessMessage from "@/app/components/SuccessMessage";
import Button from "@/app/components/Button";
import { useAuth } from "@/app/hooks/useAuth";
import Pagination from "@/app/components/Pagination";

interface Bucket {
  name: string;
  creation_date: Date;
}

export default function BucketPage() {
  useAuth();

  const [buckets, setBuckets] = useState<Bucket[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedBucket, setSelectedBucket] = useState<Bucket | null>(null);
  const [inputBucketName, setInputBucketName] = useState("");
  const [showSuccessModal, setShowSuccessModal] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [itemsPerPage] = useState(10);
  const [successMessage, setSuccessMessage] = useState("");

  const router = useRouter();

  const fetchBuckets = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const data = await callGetAllBuckets();
      setBuckets(data.buckets);
    } catch {
      setError("Failed to fetch buckets");
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchBuckets();
  }, [fetchBuckets]);

  const handleDelete = async () => {
    if (selectedBucket) {
      try {
        await callRemoveBucket(selectedBucket.name);
        setBuckets((prevBuckets) =>
          prevBuckets.filter((bucket) => bucket.name !== selectedBucket.name)
        );
        setIsModalOpen(false);
        setSuccessMessage(
          `Bucket "${selectedBucket.name}" was successfully deleted!`
        );
        setShowSuccessModal(true);
      } catch {
        setError(`Failed to delete bucket: ${selectedBucket.name}.`);
      }
    }
  };

  const handleRowClick = (bucketName: string) => {
    router.push(`/pages/dataset?bucket=${bucketName}`);
  };

  const handleCreateBucket = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      await callCreateBucket(inputBucketName);
      fetchBuckets();
      setInputBucketName("");
      setSuccessMessage(
        `Bucket "${inputBucketName}" was successfully created!`
      );
      setShowSuccessModal(true);
    } catch {
      setError(`Failed to create bucket: ${inputBucketName}.`);
    }
  };

  const isCreateDisabled =
    inputBucketName.length < 3 || inputBucketName.length > 63;

  const indexOfLastItem = currentPage * itemsPerPage;
  const indexOfFirstItem = indexOfLastItem - itemsPerPage;
  const currentBuckets = buckets.slice(indexOfFirstItem, indexOfLastItem);

  if (loading) {
    return <div>Loading...</div>;
  }

  return (
    <div className="p-5 text-lg font-semibold text-left rtl:text-right text-gray-900 bg-white dark:text-white dark:bg-gray-800">
      <div className="max-w-sm mx-auto my-4">
        <h1 className="text-2xl pb-5 text-center">Bucket page</h1>

        <form onSubmit={handleCreateBucket}>
          <InputField
            label="Bucket name"
            type="text"
            id="bucket"
            name="bucket"
            required
            value={inputBucketName}
            onChange={(value) => setInputBucketName(value)}
            pattern="^[a-z0-9]{0,63}$"
          />
          <Button type="submit" disabled={isCreateDisabled}>
            Create
          </Button>
        </form>
      </div>

      <Table
        headers={["Bucket Name", "Creation Date", "Remove"]}
        caption="Buckets"
        description="A bucket is similar to a folder or directory in a filesystem, where each bucket can hold an arbitrary number of objects. In our case, the bucket contains image datasets, which are collections of images that we can use for processing or analysis."
      >
        {currentBuckets.map((bucket, index) => (
          <TableRow key={index} onClick={() => handleRowClick(bucket.name)}>
            <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
              {bucket.name}
            </td>
            <td className="px-6 py-4">
              {new Date(bucket.creation_date).toLocaleString()}
            </td>
            <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
              <button
                onClick={(e) => {
                  e.stopPropagation();
                  setSelectedBucket(bucket);
                  setIsModalOpen(true);
                }}
                className="font-medium text-blue-600 dark:text-blue-500 hover:underline"
              >
                <IoTrashOutline className="w-5 h-5 text-red-500 dark:text-white mr-1" />
              </button>
            </td>
          </TableRow>
        ))}
      </Table>

      <Pagination
        totalItems={buckets.length}
        itemsPerPage={itemsPerPage}
        currentPage={currentPage}
        onPageChange={setCurrentPage}
      />

      <RemoveDialog
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        onConfirm={handleDelete}
        name={selectedBucket?.name || ""}
        error={error}
        onErrorClose={() => setError("")}
      />

      {showSuccessModal && (
        <SuccessMessage
          onClose={() => setShowSuccessModal(false)}
          message={successMessage}
        />
      )}
    </div>
  );
}
