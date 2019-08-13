package uk.ac.ox.map.osm;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

@XmlRootElement
public class Way {
    String id;
    List<Nd> nds;
    List<Tag> tags;

    public String getId() {
        return id;
    }

    @XmlAttribute
    public void setId(String id) {
        this.id = id;
    }

    public List<Nd> getNds() {
        return nds;
    }

    @XmlElement(name = "nd")
    public void setNds(List<Nd> nds) {
        this.nds = nds;
    }

    public List<Tag> getTags() {
        return tags;
    }

    @XmlElement(name = "tag")
    public void setTags(List<Tag> tags) {
        this.tags = tags;
    }
}
