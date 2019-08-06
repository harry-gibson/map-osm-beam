package uk.ac.ox.map.osm;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
public class Node {
    int id;
    double lat;
    double lon;
    List<Tag> tags;

    public int getId() {
        return id;
    }

    public double getLat() { return lat;}

    public double getLon() {return lon;}

    public List<Tag> getTags() {
        return tags;
    }

    @XmlAttribute
    public void setId(int id) {
        this.id = id;
    }

    @XmlAttribute
    public void setLat(double lat) {this.lat = lat;}

    @XmlAttribute
    public void setLon(double lon){this.lon = lon;}

    @XmlElement(name = "tag")
    public void setTags(List<Tag> tags) {
        this.tags = tags;
    }
}
