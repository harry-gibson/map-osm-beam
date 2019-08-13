package uk.ac.ox.map.osm;

import com.google.common.collect.Lists;
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
import org.geotools.grid.GridFeatureBuilder;
import org.geotools.grid.Grids;
import org.geotools.referencing.crs.DefaultGeographicCRS;

import org.locationtech.jts.geom.*;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;


public class OSMToRoadClassificationSurface {
    static final int GRID_PIXEL_SIZE_IN_ARC_SECS = 30;
    static GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

    static class ExtractWaySegmentClassificationFn extends DoFn<Way, KV<String, String>> {
        @ProcessElement
        public void processElement(@Element Way element, OutputReceiver<KV<String, String>> receiver) {
            if (Optional.ofNullable(element.getTags()).isPresent()) {
                List<Tag> tags = element.getTags();
                String highway = null;
                for (Tag tag : tags) {
                    if (tag.k.equalsIgnoreCase("highway")) {
                        if(Optional.ofNullable(tag.v).isPresent()){
                            if(!tag.v.equalsIgnoreCase("proposed")){
                                highway = tag.v;
                            }
                        }
                        break;
                    }
                }
                if (highway != null) {
                    if (Optional.ofNullable(element.getNds()).isPresent()) {
                        List<Nd> nds = element.getNds();
                        for (int i = 0; i < nds.size() - 1; i++) {
                            String start = nds.get(i).ref;
                            String end = nds.get(i + 1).ref;
                            int code = fromString(highway);
                            if (code > 0) {
                                StringBuilder stringBuilder = new StringBuilder();
                                stringBuilder
                                        .append(start).append(",")
                                        .append(end).append(",")
                                        .append(i).append(",")
                                        .append(element.id).append(",")
                                        .append(code);
                                receiver.output(KV.of(String.valueOf(start), stringBuilder.toString()));
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
            stringBuilder.append(element.id).append(",").append(element.lat).append(",").append(element.lon);
            receiver.output(KV.of(String.valueOf(element.id), stringBuilder.toString()));
        }
    }

    public static class ExtractWaySegments
            extends PTransform<PCollection<Way>, PCollection<KV<String, String>>> {
        @Override
        public PCollection<KV<String, String>> expand(PCollection<Way> ways) {
            return ways.apply(ParDo.of(new ExtractWaySegmentClassificationFn()));
        }
    }

    public static class ExtractNodes
            extends PTransform<PCollection<Node>, PCollection<KV<String, String>>> {
        @Override
        public PCollection<KV<String, String>> expand(PCollection<Node> nodes) {
            return nodes.apply(ParDo.of(new ExtractNodesFn()));
        }
    }

    public static PCollection<KV<String, WaySegment>> joinNodesToWaySegments(
            final PCollection<KV<String, String>> ways,
            final PCollection<KV<String, String>> nodes) {

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
                            String start = wayString[0];
                            String end = wayString[1];
                            String sequence = wayString[2];
                            String wayId = wayString[3];
                            String highway = wayString[4];
                            for (String rightValue : nodesIterable) {
                                String[] nodeString = rightValue.split(",");
                                String nodeId = nodeString[0];
                                Double lat = Double.valueOf(nodeString[1]);
                                Double lon = Double.valueOf(nodeString[2]);
                                if (lat != 0.0d || lon != 0.0d) {
                                    StringBuilder stringBuilder = new StringBuilder();
                                    stringBuilder
                                            .append(start).append(",")
                                            .append(end).append(",")
                                            .append(lat).append(",")
                                            .append(lon).append(",")
                                            .append(sequence).append(",")
                                            .append(wayId).append(",")
                                            .append(highway);
                                    c.output(KV.of(end, stringBuilder.toString()));
                                }
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
                new DoFn<KV<String, CoGbkResult>, KV<String, WaySegment>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        KV<String, CoGbkResult> e = c.element();

                        Iterable<String> waysIterable = e.getValue().getAll(waysWithStartLocationTuple);
                        Iterable<String> nodesIterable = e.getValue().getAll(nodesTuple);

                        for (String leftValue : waysIterable) {
                            String[] wayString = leftValue.split(",");
                            String start = wayString[0];
                            String end = wayString[1];
                            Double startLat = Double.valueOf(wayString[2]);
                            Double startLon = Double.valueOf(wayString[3]);
                            Integer sequence = Integer.valueOf(wayString[4]);
                            String wayId = wayString[5];
                            Integer highway = Integer.valueOf(wayString[6]);
                            for (String rightValue : nodesIterable) {
                                String[] nodeString = rightValue.split(",");
                                String nodeId = nodeString[0];
                                Double endLat = Double.valueOf(nodeString[1]);
                                Double endLon = Double.valueOf(nodeString[2]);
                                if (endLat != 0.0d || endLon != 0.0d) {
                                    WaySegment waySegment = new WaySegment(startLat,startLon,endLat,endLon,sequence,highway);
                                    c.output(KV.of(wayId, waySegment));
                                }
                            }
                        }
                    }
                }));
    }

    static class BuildWayFn extends DoFn<KV<String, Iterable<WaySegment>>, KV<String, Integer>> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String wayId = c.element().getKey();
            Iterable<WaySegment> waySegments = c.element().getValue();
            List<Coordinate> coordinatesList = Lists.newArrayList();
            int highwayCode = 15;
            for(WaySegment waySegment: waySegments){
                if (waySegment.sequence == 0){
                    Coordinate start = new Coordinate(waySegment.getStartLon(), waySegment.getStartLat());
                    start.setZ(0.0);
                    coordinatesList.add(start);
                }
                Coordinate end = new Coordinate(waySegment.getEndLon(), waySegment.getEndLat());
                end.setZ(Double.valueOf(waySegment.sequence + 1));
                coordinatesList.add(end);
                if(waySegment.getHighwayCode() < highwayCode){
                    highwayCode = waySegment.getHighwayCode();
                }
            }
            if (coordinatesList.size() >= 2){
                Coordinate[] coordinates = new Coordinate[coordinatesList.size()];
                coordinates = coordinatesList.toArray(coordinates);
                Arrays.sort(coordinates, new Comparator<Coordinate>() {
                    @Override
                    public int compare(Coordinate o1, Coordinate o2) {
                        return Double.valueOf(o1.getZ()).compareTo(Double.valueOf(o2.getZ()));
                    }
                });

                LineString lineString = geometryFactory.createLineString(coordinates);
                SimpleFeatureSource simpleFeatureSource = buildGridInfo(lineString);
                try {
                    SimpleFeatureIterator iterator = simpleFeatureSource.getFeatures().features();
                    while (iterator.hasNext()) {
                        SimpleFeature feature = iterator.next();
                        Polygon polygon = (Polygon) feature.getDefaultGeometry();

                        Envelope polygonEnvelopeInternal = polygon.getEnvelopeInternal();
                        double x = polygonEnvelopeInternal.getMinX() + (polygonEnvelopeInternal.getWidth() / 2);
                        double y = polygonEnvelopeInternal.getMinY() + (polygonEnvelopeInternal.getHeight() / 2);
                        StringBuilder stringBuilderK = new StringBuilder();
                        stringBuilderK.append(y).append(",").append(x);
                        c.output(KV.of(stringBuilderK.toString(), highwayCode));

                    }
                } catch (IOException ioE) {
                    ioE.printStackTrace();
                }
            }
        }
    }

    static class MaxPixelFlow
            extends PTransform<PCollection<KV<String, WaySegment>>, PCollection<KV<String, Integer>>> {
        @Override
        public PCollection<KV<String, Integer>> expand(PCollection<KV<String, WaySegment>> waySegments) {
            PCollection<KV<String, Iterable<WaySegment>>> ways = waySegments.apply(GroupByKey.<String, WaySegment>create());
            return ways.apply(ParDo.of(new BuildWayFn()));
        }
    }


    public static class FormatAsTextFn extends SimpleFunction<KV<String, String>, String> {
        @Override
        public String apply(KV<String, String> input) {
            return input.getKey() + "," + input.getValue();
        }
    }

    public static class FormatWaySegmentAsTextFn extends SimpleFunction<KV<String, WaySegment>, String> {
        @Override
        public String apply(KV<String, WaySegment> input) {
            return input.getKey() + "," + input.getValue();
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

        PCollection<Node> osmNodes = p.apply(XmlIO.<Node>read()
                .from(options.getInputFile())
                .withCompression(Compression.BZIP2)
                .withRootElement("osm")
                .withRecordElement("node")
                .withRecordClass(Node.class));

        PCollection<KV<String, String>> nodes = osmNodes.apply(new ExtractNodes());

        PCollection<Way> osmWays = p.apply(XmlIO.<Way>read()
                .from(options.getInputFile())
                .withCompression(Compression.BZIP2)
                .withRootElement("osm")
                .withRecordElement("way")
                .withRecordClass(Way.class));
        PCollection<KV<String, String>> waySegments = osmWays.apply(new ExtractWaySegments());
        PCollection<KV<String, WaySegment>> waySegmentsByWayId = joinNodesToWaySegments(waySegments, nodes);
        waySegmentsByWayId.apply(new MaxPixelFlow()).apply(Min.<String>integersPerKey()).apply(MapElements.via(new FormatPixelAsTextFn()))
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
        return Grids.createSquareGrid(gridBounds, GRID_PIXEL_SIZE_IN_ARC_SECS * (1.0 / 3600.0), -1, builder);
    }

    public static int fromString(String highwayClass) {
        int valueOf;
        switch (highwayClass) {
            case "motorway":
                valueOf = 1;
                break;
            case "trunk":
                valueOf = 2;
                break;
            case "railroad":
                valueOf = 3;
                break;
            case "primary":
                valueOf = 4;
                break;
            case "secondary":
                valueOf = 5;
                break;
            case "tertiary":
                valueOf = 6;
                break;
            case "motorway link":
                valueOf = 7;
                break;
            case "primary link":
                valueOf = 8;
                break;
            case "unclassified":
                valueOf = 9;
                break;
            case "road":
                valueOf = 10;
                break;
            case "residential":
                valueOf = 11;
                break;
            case "service":
                valueOf = 12;
                break;
            case "track":
                valueOf = 13;
                break;
            case "pedestrian":
                valueOf = 14;
                break;
            default:
                valueOf = 15;
                break;

        }
        return valueOf;
    }
}
