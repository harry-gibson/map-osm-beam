package uk.ac.ox.map.osm;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class WaySegment {
    @Nullable Double startLat;
    @Nullable Double startLon;
    @Nullable Double endLat;
    @Nullable Double endLon;
    @Nullable Integer sequence;
    @Nullable Integer highwayCode;

    public WaySegment() {
    }

    public WaySegment(Double startLat, Double startLon, Integer sequence, Integer highwayCode) {
        this.startLat = startLat;
        this.startLon = startLon;
        this.sequence = sequence;
        this.highwayCode = highwayCode;
    }

    public WaySegment(Double startLat, Double startLon, Double endLat, Double endLon, Integer sequence, Integer highwayCode) {
        this.startLat = startLat;
        this.startLon = startLon;
        this.endLat = endLat;
        this.endLon = endLon;
        this.sequence = sequence;
        this.highwayCode = highwayCode;
    }

    public Double getStartLat() {
        return startLat;
    }

    public Double getStartLon() {
        return startLon;
    }

    public Double getEndLat() {
        return endLat;
    }

    public Double getEndLon() {
        return endLon;
    }

    public Integer getSequence() {
        return sequence;
    }

    public Integer getHighwayCode() {
        return highwayCode;
    }

    @Override
    public String toString() {
        return super.toString();
    }
}
