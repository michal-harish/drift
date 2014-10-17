package net.imagini.aim;

public enum Protocol {
    BINARY(0), LOADER(1), QUERY(2);
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