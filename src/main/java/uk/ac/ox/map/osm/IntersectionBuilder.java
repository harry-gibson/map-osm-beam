package uk.ac.ox.map.osm;

import java.util.Map;
import org.geotools.grid.GridElement;
import org.geotools.grid.GridFeatureBuilder;
import org.locationtech.jts.geom.LineString;
import org.opengis.feature.simple.SimpleFeatureType;


public class IntersectionBuilder extends GridFeatureBuilder {
        final LineString source;
    int id = 0;

    public IntersectionBuilder(SimpleFeatureType type, LineString source) {
        super(type);
        this.source = source;
    }

    public void setAttributes(GridElement el, Map<String, Object> attributes) {
        attributes.put("id", ++id);
    }

    @Override
    public boolean getCreateFeature(GridElement el) {
        return el.toGeometry().intersects(source);
    }
}
