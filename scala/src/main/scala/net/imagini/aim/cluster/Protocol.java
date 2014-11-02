package net.imagini.aim.cluster;

public enum Protocol {
    RESERVED(0), 
    LOADER_LOCAL(10), LOADER_DISTRIBUTED(11),
    QUERY_LOCAL(20), QUERY_DISTRIBUTED(21);
    public final int id;

    private Protocol(int id) {
        this.id = id;
    }

    static public Protocol get(int id) {
        for (Protocol p : Protocol.values())
            if (p.id == id)
                return p;
        return null;
    }
}