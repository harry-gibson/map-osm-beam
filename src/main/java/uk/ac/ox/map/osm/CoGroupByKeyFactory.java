package uk.ac.ox.map.osm;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class CoGroupByKeyFactory {

    public static <K> PTransform<KeyedPCollectionTuple<K>, PCollection<KV<K, CoGbkResult>>> obtain(String runner) {
        return CoGroupByKey.<K>create();
    }
}
