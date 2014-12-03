package net.imagini.drift.cluster;

public enum Protocol {
    RESERVED(0), 
    LOADER_USER(10), LOADER_INTERNAL(11),
    QUERY_USER(20), QUERY_INTERNAL(21);
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