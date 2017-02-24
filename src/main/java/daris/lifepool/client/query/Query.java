package daris.lifepool.client.query;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SortedSet;

import com.pixelmed.dicom.AttributeTag;

import arc.mf.client.ServerClient;
import arc.xml.XmlStringWriter;

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

    Query() {
        this(null);
    }

    public void putElement(QueryElement element) {
        if (element != null) {
            _elements.put(element.attributeTag(), element);
        }
    }

    public void putElement(AttributeTag tag, Predicate... predicates) {
        if (predicates == null || predicates.length == 0) {
            if (_elements.containsKey(tag)) {
                _elements.remove(tag);
            }
            return;
        }
        putElement(new QueryElement(tag, predicates));
    }

    public void putElement(AttributeTag tag, Operator op, String value) {
        putElement(tag, new Predicate(null, op, value));
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

    public void execute(ServerClient.Connection cxn, String pid, SortedSet<String> resultAssetIds) throws Throwable {
        StringBuilder sb = new StringBuilder();
        sb.append("(cid starts with '").append(pid).append("') and (");
        sb.append(toString()).append(")");
        XmlStringWriter w = new XmlStringWriter();
        w.add("where", sb.toString());
        w.add("action", "get-id");
        w.add("size", "infinity");
        Collection<String> ids = cxn.execute("asset.query", w.document()).values("id");
        if (ids != null && !ids.isEmpty()) {
            resultAssetIds.addAll(ids);
        }
    }

}
