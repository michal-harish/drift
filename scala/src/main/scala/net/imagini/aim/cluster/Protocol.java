package net.imagini.aim.cluster;

public enum Protocol {
    DATA(0), LOADER(1), QUERY(2), MAPREDUCE(3);
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