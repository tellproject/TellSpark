package ch.ethz;

/**
 */
public class Customer {
    private final int cId;
    private final int dId;
    private final int wId;
    private final String cFirst;
    private final String cLast;

    Customer (int c, int d, int w, String first, String last) {
        cId = c;
        dId = d;
        wId = w;
        cFirst = first;
        cLast = last;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        return sb.append("{").append("cId").append(":").append(cId).append(",")
                .append("dId").append(":").append(dId).append(",")
                .append("wId").append(":").append(wId).append(",")
                .append("cFirst").append(":").append(cFirst).append(",")
                .append("cLast").append(":").append(cLast).append("}").toString();
    }

    public int getcId() {
        return cId;
    }

    public int getdId() {
        return dId;
    }

    public int getwId() {
        return wId;
    }

    public String getcFirst() {
        return cFirst;
    }

    public String getcLast() {
        return cLast;
    }
}
