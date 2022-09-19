SELECT groupBitmapAnd(bitmapBuild([to_int32(1)])), groupBitmapOr(bitmapBuild([to_int32(1)])), groupBitmapXor(bitmapBuild([to_int32(1)])) FROM cluster(test_cluster_two_shards, numbers(10));
