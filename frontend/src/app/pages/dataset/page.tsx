"use client";

import RemoveDialog from "@/app/components/RemoveDialog";
import { useEffect, useState, useCallback } from "react";
import { IoTrashOutline } from "react-icons/io5";
import {
  callGetAllDatasets,
  callRemoveDataset,
} from "@/app/services/dataset/datasetService";
import { callGetAllBuckets } from "@/app/services/bucket/bucketService";

interface Bucket {
  name: string;
  creation_date: Date;
}

export default function DatasetPage() {
  const [datasets, setDatasets] = useState<string[]>([]); // Zmenené na pole reťazcov
  const [buckets, setBuckets] = useState<Bucket[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null); // Zmenené na reťazec
  const [selectedBucket, setSelectedBucket] = useState<string>("");

  const fetchBuckets = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const data = await callGetAllBuckets();
      console.log("Buckets API response: ", data);
      setBuckets(data?.buckets || []);
    } catch {
      setError("Failed to fetch buckets");
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchDatasets = useCallback(async () => {
    if (!selectedBucket) return;

    setLoading(true);
    setError("");
    try {
      const data = await callGetAllDatasets(selectedBucket);
      console.log("Datasets API response: ", data);
      setDatasets(data?.databases || []);
    } catch {
      setError("Failed to fetch datasets");
    } finally {
      setLoading(false);
    }
  }, [selectedBucket]);

  useEffect(() => {
    fetchBuckets();
  }, [fetchBuckets]);

  useEffect(() => {
    fetchDatasets();
  }, [fetchDatasets]);

  const handleDelete = async () => {
    if (selectedDataset && selectedBucket) {
      try {
        await callRemoveDataset(selectedBucket, selectedDataset);

        setDatasets((prevDatasets) =>
          prevDatasets.filter((dataset) => dataset !== selectedDataset)
        );

        setIsModalOpen(false);
      } catch {
        setError(`Failed to delete dataset: ${selectedDataset}`);
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
      <div className="p-5 text-lg font-semibold text-left rtl:text-right text-gray-900 bg-white dark:text-white dark:bg-gray-800">
        <label
          htmlFor="bucket-select"
          className="block mb-2 text-sm font-medium text-gray-900 dark:text-white"
        >
          Select Bucket
        </label>
        <select
          id="bucket-select"
          value={selectedBucket}
          onChange={(e) => setSelectedBucket(e.target.value)}
          className="bg-gray-50 border border-gray-300 text-gray-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-gray-700 dark:border-gray-600 dark:placeholder-gray-400 dark:text-white dark:focus:ring-blue-500 dark:focus:border-blue-500"
        >
          <option value="">Choose a bucket</option>
          {buckets.map((bucket) => (
            <option key={bucket.name} value={bucket.name}>
              {bucket.name}
            </option>
          ))}
        </select>
      </div>
      <table className="w-[80%] mx-auto my-4 text-sm text-left rtl:text-right text-gray-500 dark:text-gray-400">
        <caption className="p-5 text-lg font-semibold text-left rtl:text-right text-gray-900 bg-white dark:text-white dark:bg-gray-800">
          Datasets
          <p className="mt-1 text-sm font-normal text-gray-500 dark:text-gray-400">
            A dataset is a collection of data, typically in a structured format,
            that can be used for analysis or processing.
          </p>
        </caption>
        <thead className="text-xs text-gray-700 uppercase bg-gray-50 dark:bg-gray-700 dark:text-gray-400">
          <tr>
            <th scope="col" className="px-6 py-3">
              Dataset Name
            </th>
            <th scope="col" className="px-6 py-3">
              Remove
            </th>
          </tr>
        </thead>
        <tbody>
          {datasets.map((dataset, index) => (
            <tr
              key={index}
              className="bg-white border-b dark:bg-gray-800 dark:border-gray-700 border-gray-200"
            >
              <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                {dataset}
              </td>
              <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                <button
                  onClick={() => {
                    setSelectedDataset(dataset);
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
        name={selectedDataset || ""}
      />
    </div>
  );
}
