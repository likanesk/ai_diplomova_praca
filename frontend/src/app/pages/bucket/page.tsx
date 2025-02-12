"use client";

import RemoveDialog from "@/app/components/RemoveDialog";
import { useEffect, useState, useCallback } from "react";
import { IoTrashOutline } from "react-icons/io5";
import {
  callGetAllBuckets,
  callRemoveBucket,
} from "@/app/services/bucket/bucketService";
import Table from "@/app/components/Table";
import TableRow from "@/app/components/TableRow";
import { useRouter } from "next/navigation";

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
      } catch {
        setError(`Failed to delete bucket: ${selectedBucket.name}`);
      }
    }
  };

  const handleRowClick = (bucketName: string) => {
    router.push(`/pages/dataset?bucket=${bucketName}`);
  };

  if (loading) {
    return <div>Loading...</div>;
  }

  if (error) {
    return <div>{error}</div>;
  }

  return (
    <div className="relative overflow-x-auto shadow-md sm:rounded-lg">
      <Table
        headers={["Bucket Name", "Creation Date", "Remove"]}
        caption="Buckets"
        description="A bucket is similar to a folder or directory in a filesystem, where
            each bucket can hold an arbitrary number of objects. In our case,
            the bucket contains image datasets, which are collections of images
            that we can use for processing or analysis."
      >
        {buckets.map((bucket, index) => (
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

      <RemoveDialog
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        onConfirm={handleDelete}
        name={selectedBucket?.name || ""}
      />
    </div>
  );
}
