package uk.ac.ox.map.osm;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.xml.XmlIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.grid.Envelopes;
import org.geotools.grid.GridFeatureBuilder;
import org.geotools.grid.Grids;
import org.geotools.referencing.crs.DefaultGeographicCRS;

import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.List;
import java.util.Optional;


public class OSMToRoadClassificationSurface {
    static final int GRID_PIXEL_SIZE_IN_ARC_SECS = 30;
    static GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

    static class ExtractWaysClassificationFn extends DoFn<Way, KV<String, String>> {
        @ProcessElement
        public void processElement(@Element Way element, OutputReceiver<KV<String, String>> receiver) {
            if(Optional.ofNullable(element.getTags()).isPresent()){
                List<Tag> tags = element.getTags();
                String highway = null;
                for (Tag tag: tags){
                    if(tag.k.equalsIgnoreCase("highway")){
                        highway = tag.v;
                        break;
                    }
                }
                if(highway != null){
                    if(highway.equalsIgnoreCase("trunk")){
                        System.out.println(element);
                    }
                    if(Optional.ofNullable(element.getNds()).isPresent()){
                        List<Nd> nds = element.getNds();
                        for (int i = 0; i < nds.size() - 2; i++) {
                            int start = nds.get(i).ref;
                            int end = nds.get(i + 1).ref;
                            int code = fromString(highway);
                            if(code > 0){
                                StringBuilder stringBuilder = new StringBuilder();
                                stringBuilder.append(end).append(",").append(code);
                                receiver.output(KV.of(String.valueOf(start),stringBuilder.toString()));
                            }

                        }
                    }
                }
            }
        }
    }

    static class ExtractNodesFn extends DoFn<Node, KV<String, String>> {
        @ProcessElement
        public void processElement(@Element Node element, OutputReceiver<KV<String, String>> receiver) {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(element.lat).append(",").append(element.lon);
            if(Optional.ofNullable(element.getTags()).isPresent()){
                List<Tag> tags = element.getTags();
                String highway = null;
                for (Tag tag: tags){
                    if(tag.k.equalsIgnoreCase("highway")){
                        highway = tag.v;
                        break;
                    }
                }
                if(highway != null){
                    receiver.output(KV.of(String.valueOf(element.id), stringBuilder.toString()));
                }
            } else {
                receiver.output(KV.of(String.valueOf(element.id), stringBuilder.toString()));
            }
        }
    }

    public static class ExtractWays
            extends PTransform<PCollection<Way>, PCollection<KV<String, String>>> {
        @Override
        public PCollection<KV<String, String>> expand(PCollection<Way> ways) {
            return ways.apply(ParDo.of(new ExtractWaysClassificationFn()));
        }
    }

    public static class ExtractNodes
            extends PTransform<PCollection<Node>, PCollection<KV<String, String>>> {
        @Override
        public PCollection<KV<String, String>> expand(PCollection<Node> nodes) {
            return nodes.apply(ParDo.of(new ExtractNodesFn()));
        }
    }

    public static PCollection<KV<String, Integer>> join(
            final PCollection<KV<String, String>> ways,
            final PCollection<KV<String, String>> nodes){

        final TupleTag<String> waysTuple = new TupleTag<>();
        final TupleTag<String> nodesTuple = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> startCoGbkResultCollection =
                KeyedPCollectionTuple.of(waysTuple, ways)
                        .and(nodesTuple, nodes)
                        .apply(CoGroupByKey.create());

        PCollection<KV<String, String>> waysWithStartLocation = startCoGbkResultCollection.apply(ParDo.of(
                new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, CoGbkResult> e = c.element();

                        Iterable<String> waysIterable = e.getValue().getAll(waysTuple);
                        Iterable<String> nodesIterable = e.getValue().getAll(nodesTuple);

                        for (String leftValue : waysIterable) {
                            String[] wayString = leftValue.split(",");
                            String end = wayString[0];
                            String highway = wayString[1];
                            for (String rightValue : nodesIterable) {
                                String[] nodeString = rightValue.split(",");
                                String lat = nodeString[0];
                                String lon = nodeString[1];
                                StringBuilder stringBuilder = new StringBuilder();
                                stringBuilder.append(e.getKey()).append(",")
                                        .append(lat).append(",")
                                        .append(lon).append(",")
                                        .append(highway);
                                c.output(KV.of(end, stringBuilder.toString()));
                            }
                        }
                    }
                }));
        final TupleTag<String> waysWithStartLocationTuple = new TupleTag<>();

        PCollection<KV<String, CoGbkResult>> endCoGbkResultCollection =
                KeyedPCollectionTuple.of(waysWithStartLocationTuple, waysWithStartLocation)
                        .and(nodesTuple, nodes)
                        .apply(CoGroupByKey.create());

        return endCoGbkResultCollection.apply(ParDo.of(
                new DoFn<KV<String, CoGbkResult>, KV<String,Integer>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, CoGbkResult> e = c.element();

                        Iterable<String> waysIterable = e.getValue().getAll(waysWithStartLocationTuple);
                        Iterable<String> nodesIterable = e.getValue().getAll(nodesTuple);

                        for (String leftValue : waysIterable) {
                            String[] wayString = leftValue.split(",");
                            String startLat = wayString[1];
                            String startLon = wayString[2];
                            Coordinate start = new Coordinate(Double.valueOf(startLon), Double.valueOf(startLat));
                            String highway = wayString[3];
                            for (String rightValue : nodesIterable) {
                                String[] nodeString = rightValue.split(",");
                                String lat = nodeString[0];
                                String lon = nodeString[1];
                                Coordinate end = new Coordinate(Double.valueOf(lon), Double.valueOf(lat));
                                Coordinate[] coordinates = new Coordinate[] {start, end};
                                LineString lineString = geometryFactory.createLineString(coordinates);
                                SimpleFeatureSource simpleFeatureSource = buildGridInfo(lineString);
                                try {
                                    SimpleFeatureIterator iterator = simpleFeatureSource.getFeatures().features();
                                    while (iterator.hasNext()) {
                                        SimpleFeature feature = iterator.next();
                                        Polygon polygon = (Polygon) feature.getDefaultGeometry();
                                        if(polygon.intersects(lineString)){
                                            StringBuilder stringBuilder = new StringBuilder();
                                            Envelope polygonEnvelopeInternal = polygon.getEnvelopeInternal();
                                            double x = polygonEnvelopeInternal.getMinX() + (polygonEnvelopeInternal.getWidth() / 2);
                                            double y = polygonEnvelopeInternal.getMinY() + (polygonEnvelopeInternal.getHeight() / 2);
                                            stringBuilder.append(x).append(",")
                                                    .append(y);
                                            c.output(KV.of(stringBuilder.toString(), Integer.valueOf(highway)));
                                        }
                                    }
                                } catch (IOException ioE) {
                                    ioE.printStackTrace();
                                }
                            }
                        }
                    }
                }));
    }

    public static class FormatAsTextFn extends SimpleFunction<KV<String, String>, String> {
        @Override
        public String apply(KV<String, String> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    public static class FormatPixelAsTextFn extends SimpleFunction<KV<String, Integer>, String> {
        @Override
        public String apply(KV<String, Integer> input) {
            return input.getKey() + "," + input.getValue();
        }
    }

    public interface OSMToRoadClassificationSurfaceOptions extends PipelineOptions {

        @Description("Path of the file to read from")
//        @Default.String("gs://map-osm/inputs/africa/equatorial-guinea/equatorial-guinea-latest.osm.bz2")
//        @Default.String("gs://map-osm/inputs/south-america/brazil/brazil-latest.osm.bz2")
        @Default.String("gs://map-osm/map.osm.bz2")
        String getInputFile();

        void setInputFile(String value);

        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }

    static void run(OSMToRoadClassificationSurfaceOptions options) {
        Pipeline p = Pipeline.create(options);
        PCollection<Node> osmNodes= p.apply(XmlIO.<Node>read()
                .from(options.getInputFile())
                .withCompression(Compression.BZIP2)
                .withRootElement("osm")
                .withRecordElement("node")
                .withRecordClass(Node.class));

        PCollection<KV<String, String>> nodes = osmNodes.apply(new ExtractNodes());
        PCollection<Way> osmWays= p.apply(XmlIO.<Way>read()
                .from(options.getInputFile())
                .withCompression(Compression.BZIP2)
                .withRootElement("osm")
                .withRecordElement("way")
                .withRecordClass(Way.class));
        PCollection<KV<String, String>> ways = osmWays.apply(new ExtractWays());
        PCollection<KV<String, Integer>> join = join(ways, nodes);
        PCollection<KV<String, Integer>> pixels = join.apply(Min.<String>integersPerKey());
        pixels.apply(MapElements.via(new FormatPixelAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));
        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        OSMToRoadClassificationSurfaceOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(OSMToRoadClassificationSurfaceOptions.class);
        run(options);
    }

    private static SimpleFeatureSource buildGridInfo(LineString lineString) {
        Envelope lineStringEnvelopeInternal = lineString.getEnvelopeInternal();
        double minX = Math.floor(lineStringEnvelopeInternal.getMinX());
        double maxX = Math.ceil(lineStringEnvelopeInternal.getMaxX());
        double minY = Math.floor(lineStringEnvelopeInternal.getMinY());
        double maxY = Math.ceil(lineStringEnvelopeInternal.getMaxY());
        ReferencedEnvelope gridBounds =
                new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84);
        SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
        tb.setName("grid");
        tb.add(
                GridFeatureBuilder.DEFAULT_GEOMETRY_ATTRIBUTE_NAME,
                Polygon.class,
                gridBounds.getCoordinateReferenceSystem());
        tb.add("id", Integer.class);
        SimpleFeatureType TYPE = tb.buildFeatureType();

        GridFeatureBuilder builder = new IntersectionBuilder(TYPE, lineString);
        return Grids.createSquareGrid(gridBounds, GRID_PIXEL_SIZE_IN_ARC_SECS * (1.0/3600.0), -1, builder);
    }

    public static int fromString(String highwayClass) {
        int valueOf;
        switch (highwayClass) {
            case "motorway":
                valueOf = 1;
                break;
            case  "trunk":
                valueOf = 2;
                break;
            case  "railroad":
                valueOf = 3;
                break;
            case  "primary":
                valueOf = 4;
                break;
            case  "secondary":
                valueOf = 5;
                break;
            case  "tertiary":
                valueOf = 6;
                break;
            case  "motorway link":
                valueOf = 7;
                break;
            case  "primary link":
                valueOf = 8;
                break;
            case  "unclassified":
                valueOf = 9;
                break;
            case  "road":
                valueOf = 10;
                break;
            case  "residential":
                valueOf = 11;
                break;
            case  "service":
                valueOf = 12;
                break;
            case  "track":
                valueOf = 13;
                break;
            case  "pedestrian":
                valueOf = 14;
                break;
            default:
                valueOf = 15;
                break;

        }
        return valueOf;
    }
}
