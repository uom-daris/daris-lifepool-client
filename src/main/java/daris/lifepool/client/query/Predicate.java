package daris.lifepool.client.query;

import com.pixelmed.dicom.AttributeTag;

public class Predicate {
    private LogicOperator _lo;
    private Operator _op;
    private String _value;

    Predicate(LogicOperator lo, Operator op, String value) {
        _lo = lo;
        _op = op;
        _value = value;
    }

    public Operator operator() {
        return _op;
    }

    public String value() {
        return _value;
    }

    public LogicOperator logicOperator() {
        return _lo;
    }

    void save(AttributeTag tag, StringBuilder sb) {
        if (_lo != null) {
            sb.append(" ").append(_lo).append(" ");
        }
        sb.append("(xpath(daris:dicom-dateset/object/de[@tag='")
                .append(String.format("%04x%04x", tag.getGroup(), tag.getElement())).append("']/value)");
        sb.append(_op.symbol());
        sb.append("'").append(_value).append("')");
    }
}