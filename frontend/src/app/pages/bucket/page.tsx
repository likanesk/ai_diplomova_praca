"use client";

import RemoveDialog from "@/app/components/RemoveDialog";
import { useEffect, useState, useCallback } from "react";
import { IoTrashOutline } from "react-icons/io5";
import {
  callGetAllBuckets,
  callRemoveBucket,
} from "@/app/services/bucket/bucketService";

interface Bucket {
  name: string;
  creation_date: Date;
}

export default function BucketPage() {
  const [buckets, setBuckets] = useState<Bucket[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedBucket, setSelectedBucket] = useState<Bucket | null>(null);

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
      } catch {
        setError(`Failed to delete bucket: ${selectedBucket.name}`);
      }
    }
  };

  if (loading) {
    return <div>Loading...</div>;
  }

  if (error) {
    return <div>{error}</div>;
  }

  return (
    <div className="relative overflow-x-auto shadow-md sm:rounded-lg">
      <table className="w-[80%] mx-auto my-4 text-sm text-left rtl:text-right text-gray-500 dark:text-gray-400">
        <caption className="p-5 text-lg font-semibold text-left rtl:text-right text-gray-900 bg-white dark:text-white dark:bg-gray-800">
          Buckets
          <p className="mt-1 text-sm font-normal text-gray-500 dark:text-gray-400">
            A bucket is similar to a folder or directory in a filesystem, where
            each bucket can hold an arbitrary number of objects. In our case,
            the bucket contains image datasets, which are collections of images
            that we can use for processing or analysis.
          </p>
        </caption>
        <thead className="text-xs text-gray-700 uppercase bg-gray-50 dark:bg-gray-700 dark:text-gray-400">
          <tr>
            <th scope="col" className="px-6 py-3">
              Bucket Name
            </th>
            <th scope="col" className="px-6 py-3">
              Creation Date
            </th>
            <th scope="col" className="px-6 py-3">
              Remove
            </th>
          </tr>
        </thead>
        <tbody>
          {buckets.map((bucket, index) => (
            <tr
              key={index}
              className="bg-white border-b dark:bg-gray-800 dark:border-gray-700 border-gray-200"
            >
              <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                {bucket.name}
              </td>
              <td className="px-6 py-4">
                {new Date(bucket.creation_date).toLocaleString()}
              </td>
              <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                <button
                  onClick={() => {
                    setSelectedBucket(bucket);
                    setIsModalOpen(true);
                  }}
                  className="font-medium text-blue-600 dark:text-blue-500 hover:underline"
                >
                  <IoTrashOutline className="w-5 h-5 text-red-500 dark:text-white mr-1" />
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      <RemoveDialog
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        onConfirm={handleDelete}
        name={selectedBucket?.name || ""}
      />
    </div>
  );
}
