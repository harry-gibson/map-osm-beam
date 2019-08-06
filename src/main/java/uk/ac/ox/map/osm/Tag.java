package uk.ac.ox.map.osm;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

@XmlType(propOrder = {"k", "v"}, name = "tag")
public class Tag {
    String k;
    String v;

    public String getK() {
        return k;
    }

    @XmlAttribute
    public void setK(String k) {
        this.k = k;
    }

    public String getV() {
        return v;
    }

    @XmlAttribute
    public void setV(String v) {
        this.v = v;
    }

    @Override
    public String toString() {
        return "tag [ " + k + ": " + v + " ]";
    }
}

