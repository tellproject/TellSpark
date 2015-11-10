package ch.ethz;

import java.io.Serializable;

/**
 * Sample Java Bean
 */
public class Customer implements Serializable {

    private static final long serialVersionUID = 7526472295622770144L;

    private int cId;
    private long dId;
    private long wId;
    private String cFirst;
    private String cLast;

    public Customer () {}

    public Customer (int c, long d, long w, String first, String last) {
        cId = c;
        dId = d;
        wId = w;
        cFirst = first;
        cLast = last;
    }

    public void setField(int fPos, Object val) {
        switch(fPos) {
            case 0: this.cId = (int) val;
                break;
            case 1: this.dId = (long) val;
                break;
            case 2: this.wId = (long) val;
                break;
            case 3: this.cFirst = val.toString();
                break;
            case 4: this.cLast = val.toString();
                break;
        }
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

    public long getdId() {
        return dId;
    }

    public long getwId() {
        return wId;
    }

    public String getcFirst() {
        return cFirst;
    }

    public String getcLast() {
        return cLast;
    }
}
