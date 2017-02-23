package daris.lifepool.client.query;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;

import com.pixelmed.dicom.AttributeTag;

public class Query {

    private LinkedHashMap<AttributeTag, QueryElement> _elements;

    Query(List<QueryElement> elements) {
        _elements = new LinkedHashMap<AttributeTag, QueryElement>();
        if (elements != null) {
            for (QueryElement element : elements) {
                putElement(element);
            }
        }
    }

    public void putElement(QueryElement element) {
        _elements.put(element.attributeTag(), element);
    }

    public void removeElement(AttributeTag tag) {
        _elements.remove(tag);
    }

    public void save(StringBuilder sb) {
        Collection<QueryElement> elements = _elements.values();
        if (elements != null && !elements.isEmpty()) {
            boolean isFirstElement = true;
            for (QueryElement element : elements) {
                if (isFirstElement) {
                    isFirstElement = false;
                } else {
                    sb.append(" ").append(LogicOperator.AND).append(" ");
                }
                element.save(sb);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        save(sb);
        return sb.toString();
    }

}
