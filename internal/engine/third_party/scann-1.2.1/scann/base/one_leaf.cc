




{
  SingleMachineFactoryOptions opts;
  opts.parallelization_pool = StartThreadPool("scann_threadpool", training_threads - 1);



  KMeansTreePartitioner *kmeams_tree_partition = new KMeansTreePartitioner();
  TF_ASSIGN_OR_RETURN(partitioner, CreateTreeXPartitioner<float>(dataset, config, opts));


  auto partitioner_or_status = KMeansTreePartitionerFactoryPreSampledAndProjected(dataset, config, nullptr);
  unique_ptr<KMeansTreeLikePartitioner<float>> kmeans_tree_partitioner(
      dynamic_cast<KMeansTreeLikePartitioner<float>*>(partitioner.release()));


  shared_ptr<DenseDataset<float>> dense;
  vector<std::vector<DatapointIndex>> datapoints_by_token = {};

  TF_ASSIGN_OR_RETURN(auto residuals,TreeAHHybridResidual::ComputeResiduals(*dense, kmeans_tree_partitioner.get(), datapoints_by_token,
                      config.hash().asymmetric_hash().use_normalized_residual_quantization()));

}
