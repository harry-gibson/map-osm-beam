package uk.ac.ox.map.osm;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;

@XmlType(propOrder = {"ref"}, name = "nd")
public class Nd {
    String ref;

    public String getRef() {
        return ref;
    }

    @XmlAttribute
    public void setRef(String ref) {
        this.ref = ref;
    }

    @Override
    public String toString() {
        return "";
    }
}
