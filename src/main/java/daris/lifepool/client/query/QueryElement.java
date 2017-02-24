package daris.lifepool.client.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.pixelmed.dicom.AttributeTag;

public class QueryElement {

    private AttributeTag _tag;
    private List<Predicate> _predicates;

    QueryElement(AttributeTag tag, List<Predicate> predicates) {

        _tag = tag;
        if (predicates != null && !predicates.isEmpty()) {
            _predicates = new ArrayList<Predicate>(predicates);
        }
    }

    QueryElement(AttributeTag tag, Predicate... predicates) {

        _tag = tag;
        if (predicates != null && predicates.length > 0) {
            _predicates = new ArrayList<Predicate>(predicates.length);
            for (Predicate predicate : predicates) {
                _predicates.add(predicate);
            }
        }
    }

    public AttributeTag attributeTag() {
        return _tag;
    }

    public List<Predicate> predicates() {
        return Collections.unmodifiableList(_predicates);
    }

    public void save(StringBuilder sb, boolean excludeIfNull) {

        int nbPredicates = _predicates == null ? 0 : _predicates.size();
        if (nbPredicates > 1) {
            sb.append("(");
        }
        if (nbPredicates > 0) {
            for (int i = 0; i < nbPredicates; i++) {
                Predicate predicate = _predicates.get(i);
                predicate.save(_tag, sb);
            }
        } else {
            if (excludeIfNull) {
                sb.append("(xpath(daris:dicom-dateset/object/de[@tag='")
                        .append(String.format("%04x%04x", _tag.getGroup(), _tag.getElement()))
                        .append("']/value) has value");
            }
        }
        if (nbPredicates > 1) {
            sb.append(")");
        }
    }

}
