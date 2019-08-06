package uk.ac.ox.map.osm;

public enum HighwayClass {
    MOTORWAY(1),
    TRUNK(2),
    RAILROAD(3),
    PRIMARY(4),
    SECONDARY(5),
    TERTIARY(6),
    MOTORWAY_LINK(7),
    PRIMARY_LINK(8),
    UNCLASSIFIED(9),
    ROAD(10),
    RESIDENTIAL(11),
    SERVICE(12),
    TRACK(13),
    PEDESTRIAN(14),
    OTHER(15),
    NO_HIGHWAY(-1);

    private final int code;

    HighwayClass(int code) {
        this.code = code;
    }

    public int getCode(){
        return code;
    }

    public static HighwayClass fromString(String highwayClass) {
        HighwayClass valueOf;
        switch (highwayClass) {
            case "motorway":
                valueOf = MOTORWAY;
                break;
            case  "trunk":
                valueOf = TRUNK;
                break;
            case  "railroad":
                valueOf = RAILROAD;
                break;
            case  "primary":
                valueOf = PRIMARY;
                break;
            case  "secondary":
                valueOf = SECONDARY;
                break;
            case  "tertiary":
                valueOf = TERTIARY;
                break;
            case  "motorway link":
                valueOf = MOTORWAY_LINK;
                break;
            case  "unclassified":
                valueOf = UNCLASSIFIED;
                break;
            case  "road":
                valueOf = ROAD;
                break;
            case  "residential":
                valueOf = RESIDENTIAL;
                break;
            case  "service":
                valueOf = SERVICE;
                break;
            case  "track":
                valueOf = TRACK;
                break;
            case  "pedestrian":
                valueOf = PEDESTRIAN;
                break;
            case  "other":
                valueOf = OTHER;
                break;
            default:
                valueOf = NO_HIGHWAY;
                break;

        }
        return valueOf;
    }
}
