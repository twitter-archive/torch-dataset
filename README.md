Dataset
=======

The Torch Dataset library provides many convenient ways for
you to feed data into your model. It makes the acts of sampling,
reading and processing distinct so that you can effectively
mix and match solutions to fit your particular problem constraints.

Indexes
-------

All Datasets are driven by an index.
The index contains a list of all possible records and their
(optional) set of corresponding labels. We currently support four types of indexes.

   1. CSV
   2. Directory
   3. SlowFS (S3, HDFS or any "slow" file system)
   4. Tensor

CSV
---

For media we usually store the records in an index.csv file and
the actual records on a CDN for very fast retrieval during
training. Index csv files can live on the local file system or
out on a SlowFS. Just use a path like slowfs:///user/somebody/path/to/index.csv
to load an index directly off of a SlowFS.
Here is a very simple example index.csv.

```lua
   filename,label1,label2
   aa367ddd157f42baf2949f9416ca0311.jpg,XX
   07c67f7c341975cdc6a014f09be801a0.jpg,XX,XY
   0fff04dfb4a67540bff880fd7f143534.jpg,AA
   57c892b5743874b40b1ab1c7b126ab0b.jpg,XX
   d5e11c10c3e1edabae9e1713471b866f.jpg,AA,BB
```

There is a corresponding index-meta.csv that indicates where on
a CDN these images are stored.

```lua
   hostname
   https://your.super.fast.cdn.com/some/directory/128
```

Here's an example of using a index.csv file with Dataset.

```lua
   local dataset, numBatches = Dataset('index.csv')

   local getBatch, numBatches = dataset.sampledBatcher({
      batchSize = 1,
      inputDims = { 1 },
      samplerKind = 'linear',
      get = get,
      processor = processor,
   })
```

Note: numBatches above is a function, so call numBatches() when using
that variable. For non-SlowFS datasets it returns a constant number (i.e.
the number of items in the dataset). For SlowFS dataset the value returned
by numBatches() changes as it progresses through the part files, see below.

SlowFS
------

We don't always need to have a physical index to create a Dataset,
it is also possible to create a Dataset by simply pointing it at
a path in SlowFS. In this scenario the Dataset index is built by
scanning the SlowFS directory for part files. Part files are loaded
dynamically as the training/sampler progresses through them.

SlowFS Dataset supports the following samplers, more details in the Samplers section:

   -   part-linear
   -   part-linear-permutation
   -   part-permutation-permutation

Here is an example bit of Lua to create an SlowFS Dataset.

```lua
   local dataset, numBatches = Dataset('slowfs:///your/path/datasetTest')

   local getBatch, numBatches = dataset.sampledBatcher({
      batchSize = 1,
      inputDims = { 1 },
      samplerKind = 'part-linear',
      get = get,
      processor = processor,
   })
```

If you want to process data on multiple nodes, you need to specify
partition/partitions like so:

```lua
   local numPartitions = 4
   for p = 1, numPartitions do
      local dataset, numBatches = Dataset('slowfs:///your/path/datasetTest',
         { partition = p,
           partitions = numPartitions
         })
   end
```

The code above will run on 4 different nodes (partitions), each partition p
gets 1/4 of the part files in slowfs:///your/path/datasetTest.

Note: as mentioned above, numBatches above is a function. Since we don't know apriori how many
items are in the SlowFS part files (and it would be expensive to pre-compute this),
that number changes as we progress through the dataset. Thus to use dataset you
need to use a while loop like so:

```lua
   local b = 1
   while b <= numBatches() do
      local batch = getBatch()
      -- do stuff
      b = b + 1
   end
```

Note that there are some limitations when using a Dataset directly off of SlowFS.
We do not know how many records are in your dataset and that limits the ways
in which we can sample your data. We have two ways to sample.

   1. part-linear: walk the part files linearily and walk the records linearily in them from start to end.
   2. part-permutation: walk the part files linearily and create a unique non-repeating permutation of records to read.

There is no support for labeled data, although it should be coming soon.

Tensors
-------

You can create a dataset directly from one or more tensors.
This is useful for small datasets that fit entirely in memory.

```lua
   -- create a dataset with 100 items, each a 3x3 matrix
   local dataset = Dataset(torch.randn(100, 3, 3))
```

Tensor datasets can also be labeled, just pass in a table with two
tensors, x = items, y = labels.

```lua
   -- create a labeled dataset with 4 items and 2 label values (10 and 20)
   local dataset = Dataset({
      x = torch.Tensor({1,2,3,4}),
      y = torch.Tensor({20,10,10,20})
   })
```

You can also load tensors/tables of tensors from a file.
This includes files located on HDFS.

```lua
   torch.save('myDataset.th', {
      x = torch.Tensor({1,2,3,4}),
      y = torch.Tensor({20,10,10,20})
   })
   local dataset = Dataset('myDataset.th')

   -- load another one from SlowFS
   local dataset2 = Dataset('slowfs:///user/somebody/myDataset.th')
```

Processors
----------

Dataset is agnostic to the type of record you wish to train on. It
provides processor callbacks that you can implement to turn your
record into a Tensor suitable for training. There are many existing
processors to handle images and video.
We also have folks using Base64 encoded JSON, TSV and CSV files.

Samplers
--------

There are a few standard ways to sample your Dataset. Set the samplerKind option when configuring
sampled Dataset to the corresponding name.

   - linear, walk the records from start to end, looping back to the start when done.
   - uniform, randomly sample the records, never ends.
   - permutation, randomly sample the records without repeats until there are none left, then reset with a new random permutation.
   - label-uniform, same as Uniform but restricted to records with a specific label.
   - label-permutation, same as Permutation but restricted to records with a specific label.
   - label-distribution, randomly sample records according to a weighting per label, never ends.

   (SlowFS only)

   - part-linear, walk the files in my partitions and the items linearly from start to end, linearly, looping back to the start when done.
   - part-linear-permutation, walk the part files in my partition linearly, but permute the items in each file.
   - part-permutation-permutation, in each epoch, permute the filenames in my partition. Then walk the files in the permuted order *and* permute the items in each file.

Once you have your Dataset and Sampler you can ask for a sampled batcher.
This will be used to feed mini-batches into your model.

```lua
   local dataset = Dataset('slowfs:///your/path/datasetTest')
   local function processor(res, opt, input)
      print(res)
      return true
   end
   local getBatch, numBatches = dataset.sampledBatcher({
      batchSize = 1,
      samplerKind = 'linear',
      processor = processor,
   })
   local b = 1
   while b <= numBatches() do
      local batch = getBatch()
      -- do stuff
      b = b + 1
   end
```

For SlowFS datasets, use part-linear sampler. Part-linear means that each node will get a set of
part files and will linearly go through items in these files. In the example in the SlowFS Indices
section, each node gets 1/4 of the files in viewfs:///your/path/datasetTest directory. Each
node considers that 1/4 files as the total dataset, and linearly samples through those files. The
files are not reshuffled.

Multiple Indexes
----------------

Dataset can sample from multiple indexes at the same time.
This allows you to fetch items two or more places and combine them in your processor function.
The first index in the set drives the total number of batches.
The remaining indexes either need to have the same exact number of items
or they need to use samplers that have no reset function (i.e. uniform sampling).
You can provide a get function for each index and your processor function
will receive a result for each get. Normally, we do not provide get functions
and just let the Dataset figure it out from the urls and paths.
If any getter fails you will get nil for that value in your processor.
If the first getter fails, then that entire item is omitted from the batch (how it always works).

```lua
   local dataset = Dataset({
      'slowfs:///your/path/datasetTest',
      'slowfs:///your/path/datasetTest2',
   })
   local function processor(res1, res2, opt, input)
      print(res1)
      print(res2)
      return true
   end
   local getBatch, numBatches = dataset.sampledBatcher({
      batchSize = 1,
      samplerKind = { 'part-linear', 'part-linear-permutation' },   -- sample the first linearily, the second with a permutation
      processor = processor,
   })
   local b = 1
   while b <= numBatches() do
      local batch = getBatch(i)
      -- do stuff
      b = b + 1
   end
```

License
-------

Licensed under the Apache License, Version 2.0.
[See LICENSE file](LICENSE).
