package daris.lifepool.client.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.pixelmed.dicom.AttributeTag;

public class QueryElement {

    private AttributeTag _tag;
    private List<Predicate> _predicates;

    QueryElement(AttributeTag tag, Predicate... predicates) {

        _tag = tag;
        _predicates = new ArrayList<Predicate>(predicates.length);
        for (Predicate predicate : predicates) {
            _predicates.add(predicate);
        }
    }

    public AttributeTag attributeTag() {
        return _tag;
    }

    public List<Predicate> predicates() {
        return Collections.unmodifiableList(_predicates);
    }

    public void save(StringBuilder sb) {
        sb.append("(");
        int nbPredicates = _predicates.size();
        for (int i = 0; i < nbPredicates; i++) {
            Predicate predicate = _predicates.get(i);
            predicate.save(_tag, sb);
        }
        sb.append(")");
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        save(sb);
        return sb.toString();
    }

}
